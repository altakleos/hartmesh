"""Bounded operator inventory of pending file operations and durable mounts."""

import re
from collections import defaultdict

from sqlalchemy import func, select, tuple_

from app.gateway.agent_operator_contract import CommandRefused
from deerflow.persistence.agent_instances.model import AgentInstanceRow
from deerflow.persistence.spaces.files import SpaceFileOperationRow
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceMountRow

PAGE_SIZE = 200


async def pending_count(session):
    return await session.scalar(select(func.count()).select_from(SpaceFileOperationRow).where(SpaceFileOperationRow.phase == "pending"))


async def pending_page(session, after=None):
    operation = SpaceFileOperationRow
    query = (
        select(operation.space_id, operation.operation_id, operation.generation, operation.request["action"].as_string().label("action"), AgentInstanceRow.id.label("instance_id"))
        .outerjoin(AgentInstanceRow, AgentInstanceRow.home_id == operation.space_id)
        .where(operation.phase == "pending")
        .order_by(operation.space_id, operation.operation_id)
        .limit(PAGE_SIZE + 1)
    )
    if after is not None:
        if not isinstance(after, str) or re.fullmatch(r"[0-9a-f]{32}:[0-9a-f]{32}", after) is None:
            raise CommandRefused("Use the space and operation pair returned in next_after")
        query = query.where(tuple_(operation.space_id, operation.operation_id) > tuple(after.split(":")))
    rows = (await session.execute(query)).all()
    more = len(rows) > PAGE_SIZE
    rows = rows[:PAGE_SIZE]
    mounted = defaultdict(list)
    if rows:
        mounts = (
            select(SpaceMountRow.space_id, SpaceMountRow.generation, SpaceAttachmentRow.id.label("attachment_id"), SpaceAttachmentRow.incarnation, SpaceAttachmentRow.host_id, SpaceAttachmentRow.container_id, SpaceAttachmentRow.phase)
            .join(SpaceAttachmentRow, SpaceAttachmentRow.id == SpaceMountRow.attachment_id)
            .where(SpaceMountRow.space_id.in_({row.space_id for row in rows}), SpaceAttachmentRow.phase != "fenced")
            .order_by(SpaceMountRow.space_id, SpaceAttachmentRow.id)
        )
        for mount in (await session.execute(mounts)).mappings():
            attachment = dict(mount)
            mounted[attachment.pop("space_id")].append(attachment)
    operations = [{**row._mapping, "attachments": mounted[row.space_id]} for row in rows]
    return operations, f"{rows[-1].space_id}:{rows[-1].operation_id}" if more else None
