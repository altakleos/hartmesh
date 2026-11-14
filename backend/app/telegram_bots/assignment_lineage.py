"""Existing delegation references cannot turn back into a fresh private draft."""

from collections.abc import Mapping

from sqlalchemy import and_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.producer import ProductSourceRetired
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow


def admitted_basis(payload):
    basis = payload.get("work_news_basis")
    if not isinstance(basis, Mapping):
        return False
    versions = [basis, *[row.get("basis") for row in basis.get("history", []) if isinstance(row, Mapping)]]
    for version in versions:
        source = version.get("source") if isinstance(version, Mapping) else None
        work = source.get("work") if isinstance(source, Mapping) else None
        if isinstance(work, Mapping):
            root = work.get("root")
            if work.get("act") is not None or work.get("phase") not in {"draft", "expired", "discarded", "replaced", "answering"}:
                return True
            if isinstance(root, Mapping) and (root.get("operation_ids") or root.get("continuation") is not None):
                return True
    return False


async def require_fresh_draft(writer, link, root_id, *, internal=False):
    action, control = TelegramActRow, TelegramCallbackRow
    if await writer.scalar(select(action.id).where(action.kind == "delegate", action.continuation["delegate_source"]["draft_id"].as_string() == root_id).limit(1)) is not None:
        raise ProductSourceRetired()
    from app.telegram_bots.assignment_proposal_records import assignment_parent

    row = await assignment_parent(writer, link, root_id)
    if row is not None:
        if internal or admitted_basis(row.payload):
            raise ProductSourceRetired()
        if await writer.scalar(select(control.id).where(control.payload["anchor_outbox_id"].as_string() == row.id, control.payload["work_action_id"].as_string().is_not(None)).limit(1)) is not None:
            raise ProductSourceRetired()


class DelegateFamily:
    def __init__(self, root_id, parent_id):
        self.root_id, self.parent_id = root_id, parent_id

    def extend(self, query):
        self.column = len(query.column_descriptions)
        member, child = aliased(TelegramActRow), aliased(TelegramActRow)
        return (
            query.add_columns(member, child)
            .outerjoin(member, and_(member.kind == "delegate", member.continuation["delegate_source"]["draft_id"].as_string() == self.root_id))
            .outerjoin(child, and_(child.kind == "delegate", child.continuation["delegate_source"]["parent_id"].as_string() == self.parent_id))
        )

    def check(self, rows, parent):
        from app.telegram_bots.work_delegate_actions import delegation_children

        members = {row[self.column].id for row in rows if row[self.column] is not None}
        children = {row[self.column + 1].id for row in rows if row[self.column + 1] is not None}
        expected = list(delegation_children(parent))
        if len(expected) > 1 or len(set(expected)) != len(expected) or children != set(expected) or members != {parent.id, *expected}:
            raise ProductSourceRetired()


class FreshDraft:
    def __init__(self, root_id, delivery_id):
        self.root_id, self.delivery_id = root_id, delivery_id

    def extend(self, query):
        self.column = len(query.column_descriptions)
        member, control = aliased(TelegramActRow), aliased(TelegramCallbackRow)
        return (
            query.add_columns(member, control)
            .outerjoin(member, and_(member.kind == "delegate", member.continuation["delegate_source"]["draft_id"].as_string() == self.root_id))
            .outerjoin(control, and_(control.payload["anchor_outbox_id"].as_string() == self.delivery_id, control.payload["work_action_id"].as_string().is_not(None)))
        )

    def check(self, rows, delivery):
        if any(row[self.column] is not None or row[self.column + 1] is not None for row in rows) or admitted_basis(delivery.payload):
            raise ProductSourceRetired()


async def read_family(reader, root_id, parent):
    from app.telegram_bots.work_delegate_actions import delegation_children

    action = TelegramActRow
    members = set(await reader.scalars(select(action.id).where(action.kind == "delegate", action.continuation["delegate_source"]["draft_id"].as_string() == root_id)))
    children = set(await reader.scalars(select(action.id).where(action.kind == "delegate", action.continuation["delegate_source"]["parent_id"].as_string() == parent.id)))
    expected = delegation_children(parent)
    if len(expected) > 1 or members != {parent.id, *expected} or children != set(expected):
        raise ProductSourceRetired()
