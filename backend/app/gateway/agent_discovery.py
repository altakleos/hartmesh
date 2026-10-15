"""Read projections for agent screens; inventory summaries carry no grant."""

from dataclasses import asdict
from datetime import UTC

from fastapi.encoders import jsonable_encoder
from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import aliased

from deerflow.agent_instances.contract import AgentPermission
from deerflow.persistence.agent_instances.model import AgentConversationRow, AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.thread_meta.model import ThreadMetaRow


async def thread_bindings(app, thread_ids):
    authority = getattr(app.state, "agent_conversations", None)
    return await authority.bindings(thread_ids) if authority is not None else {}


def _date(value):
    return (value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)).isoformat() if value is not None else None


def _conversations(instance_id):
    # The authority clause owns its binding subquery. Keep this projection's
    # binding distinct so SQL correlation never consumes the authority's FROM.
    binding = aliased(AgentConversationRow)
    return select(ThreadMetaRow, binding.requester_id).join(binding, binding.thread_id == ThreadMetaRow.thread_id).where(binding.instance_id == instance_id)


async def conversations(app, service, instance, *, actor, limit, offset):
    authority = app.state.agent_conversations
    query = _conversations(instance.id).where(await authority.clause(actor.subject_id)).order_by(ThreadMetaRow.updated_at.desc(), ThreadMetaRow.thread_id).limit(limit).offset(offset)
    async with service._sf() as session:
        rows = (await session.execute(query)).all()
    return [{"thread_id": row.thread_id, "requester_id": requester, "title": row.display_name, "updated_at": _date(row.updated_at), "running": await app.state.run_manager.has_inflight(row.thread_id)} for row, requester in rows]


async def _home(service, instance, actor):
    if instance.home_id is None or not instance.permissions & AgentPermission.INSPECT:
        return None
    from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceMountRow
    from deerflow.spaces.attachments import AttachmentPending
    from deerflow.spaces.backings import BackingUnavailable
    from deerflow.spaces.contract import SpaceDenied
    from deerflow.spaces.service import SpaceOperationPending

    try:
        await service.files.registry.get(actor=actor, space_id=instance.home_id)
        async with service._sf() as session:
            phases = set((await session.execute(select(SpaceAttachmentRow.phase).join(SpaceMountRow, SpaceMountRow.attachment_id == SpaceAttachmentRow.id).where(SpaceMountRow.space_id == instance.home_id))).scalars())
        workspace = "running" if "active" in phases else "starting" if phases & {"pending", "fence_pending"} else "absent"
        try:
            await service.files.quota(actor=actor, space_id=instance.home_id)
            state = "available"
        except SpaceOperationPending:
            state = "busy"
        except BackingUnavailable:
            state = "unverified"
        except AttachmentPending:
            state, workspace = "available", "starting"
    except SpaceDenied:
        return None
    return {"state": state, "workspace": workspace}


async def detail(app, service, instance, *, actor):
    query = _conversations(instance.id)
    readable = query.where(await app.state.agent_conversations.clause(actor.subject_id)).subquery()
    async with service._sf() as session:
        last_active = (await session.execute(query.with_only_columns(func.max(ThreadMetaRow.updated_at)))).scalar_one()
        count = (await session.execute(select(func.count()).select_from(readable))).scalar_one()
    registry = service.residents
    run_ids = registry.in_flight(instance.id) if registry is not None else ()
    running = []
    if registry is not None and instance.permissions & AgentPermission.INSPECT:
        snapshot = registry.run_manager.task_runs("agent_instance_id")
        records = [(run_id, snapshot[run_id]) for run_id in run_ids if run_id in snapshot]
        work_ids = {row["metadata"]["agent_work_id"] for _, row in records if row["metadata"].get("agent_work_id") is not None}
        objectives = {}
        if work_ids:
            from deerflow.persistence.agent_instances.work import AgentWorkRow

            async with service._sf() as session:
                objectives = dict((await session.execute(select(AgentWorkRow.id, AgentWorkRow.objective).where(AgentWorkRow.id.in_(work_ids), AgentWorkRow.instance_id == instance.id))).all())
        for run_id, row in records:
            work_id = row["metadata"].get("agent_work_id")
            running.append({"run_id": run_id, "thread_id": row["thread_id"], "requester_id": row["metadata"].get("requester_id", row["user_id"]), "work_id": work_id, "objective": objectives.get(work_id)})
    if service.execution_probe is not None:
        state = await service.execution_probe(instance.id)
    else:
        from app.gateway.storage_spaces import execution_state

        state = await execution_state(app, instance.id)
    return {
        **jsonable_encoder(asdict(instance)),
        "activity": {"running_runs": len(run_ids), "running": running, "last_active_at": _date(last_active), "open_conversations": count},
        "home": await _home(service, instance, actor),
        "execution": {"execution": state.execution, "reason": state.reason},
    }


async def list_instances(service, *, actor, custody, limit, offset, include_deleted, include_provisioning):
    resolved = await service._human(actor)
    if custody != "company" or resolved.can_provision_company is not True:
        entries = await service.list(actor=actor, limit=limit, offset=offset, include_deleted=include_deleted, custody=custody, include_provisioning=include_provisioning)
        return [jsonable_encoder(asdict(entry)) for entry in entries]

    # Company inventory is the administrator's one additional read. A grant's
    # presence still decides whether the normal identity may be returned.
    async with service._sf() as session:
        rows = (
            await session.execute(
                select(AgentInstanceRow, AgentInstanceGrantRow.permissions)
                .outerjoin(AgentInstanceGrantRow, and_(AgentInstanceGrantRow.instance_id == AgentInstanceRow.id, AgentInstanceGrantRow.user_id == actor.subject_id))
                .where(
                    AgentInstanceRow.custody == "company",
                    or_(AgentInstanceRow.status != "deleted", AgentInstanceGrantRow.permissions.is_not(None)),
                    True if include_deleted else AgentInstanceRow.status != "deleted",
                    True if include_provisioning else AgentInstanceRow.status != "provisioning",
                )
                .order_by(AgentInstanceRow.created_at, AgentInstanceRow.id)
                .limit(limit)
                .offset(offset)
            )
        ).all()
    from app.gateway.deps import get_user_repo

    repository, names, result = get_user_repo(), {}, []
    for row, permissions in rows:
        if permissions is not None:
            result.append(jsonable_encoder(asdict(service._view(row, permissions))))
            continue
        if row.supervisor_id not in names:
            person = await repository.get_user_by_id(row.supervisor_id) if repository is not None else None
            names[row.supervisor_id] = (getattr(person, "name", None) or getattr(person, "email", None)) if person is not None else None
        result.append({"id": row.id, "name": row.name, "supervisor_name": names[row.supervisor_id], "status": row.status})
    return result
