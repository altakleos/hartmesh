"""Saved Start refusals retain their genuine admission and original SDK proof."""

from sqlalchemy import select
from sqlalchemy.orm import aliased

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.work_continuation_sources import ActivationParentProjection
from app.telegram_bots.work_digest_projection import DigestProjection
from app.telegram_bots.work_execution_sources import act_facts, execution_origin_rows, qualify_execution_origin, require_execution_scope
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_start_actions import answered_calls
from app.telegram_bots.work_start_conversation_sources import ContextParentProjection
from app.telegram_bots.work_start_conversations import context_plan, require_context_child
from app.telegram_bots.work_start_refresh import prepare_start_carrier
from app.telegram_bots.work_view_origins import acknowledged_view
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


def conclusive_start(link, act, root=None):
    require_execution_scope(link, root if root is not None else act)
    if root is not None:
        require_context_child(root, act)
        marker = context_plan(root)
        if root.state not in {"refused", "uncertain"} or root.state == "uncertain" and marker["phase"] != "ended" or root.reason != marker["reason"] or root.cancel_requested_at is not None:
            raise ProductSourceRetired()
        answered_calls(root)
    if act.kind != ("activate_context" if root is not None else "activate") or act.state not in {"refused", "conflict"} or act.reason != "awaiting-review" or act.cancel_requested_at is not None or act.attempts < 1:
        raise ProductSourceRetired()
    answered_calls(act)
    if root is None and (act.continuation or {}).get("activation_context") is not None:
        raise ProductSourceRetired()


def signature(act, root):
    if root is None:
        return digest(act_facts(act))
    original = act_facts(root)
    # Receipt-only deadline reconciliation may end this original family and
    # mark its outer journal uncertain. The exact refused child still owns the
    # same manual reply; fresh projection reads validate those lifecycle facts.
    original.pop("state")
    original["continuation"]["activation_context"].pop("phase")
    return digest([original, act_facts(act)])


class JournalStartProof:
    def __init__(self, chat, link, act, callback, parent, original, origin, root):
        self.chat, self.link, self.act, self.root = chat, link, act, root
        self.callback_id, self.parent_id = callback.id, parent.id
        self.original, self.signature = original, signature(act, root)
        self.parent = ContextParentProjection(act, for_effect=False) if root is not None else ActivationParentProjection(act, for_effect=False)
        self.anchor_signature = origin["signature"]

    @classmethod
    async def read(cls, chat, link, act_id):
        act = await chat.host.acts._work_record(act_id)
        require_execution_scope(link, act)
        marker = context_plan(act) if (act.continuation or {}).get("activation_context") is not None else None
        root = act if marker is not None else None
        if root is not None:
            act = await chat.host.acts._work_record(marker["child_id"])
        conclusive_start(link, act, root)
        async with chat.host.outbox.sessions() as reader:
            journal = await reader.get(TelegramActRow, act.id)
            values = await execution_origin_rows(reader, journal)
            callback, parent, _controls, _primary, actual_root = values
            if callback is None or parent is None:
                raise ProductSourceRetired()
            if root is not None and (actual_root is None or signature(act, actual_root) != signature(act, root)):
                raise ProductSourceRetired()
            origin = qualify_execution_origin(link, act, callback, parent, _controls, activation_parent=_primary, context_root=actual_root)
            original = acknowledged_view(link, parent, callback.payload.get("presentation_version")) if callback.kind == "work_start" else _item(parent)
        proof = cls(chat, link, act, callback, parent, original, origin, root)
        await proof.parent.prepare(chat.host.acts, link)
        companions = [proof]
        if callback.kind == "work_start":
            companions.append(await prepare_start_carrier(chat.views.reader, link, parent, original, activation_id=root.id if root is not None else act.id))
        proof.projection = DigestProjection(link, companions)
        return proof

    def extend(self, query):
        query = self.parent.extend(query)
        self.column = len(query.column_descriptions)
        act, parent, control = aliased(TelegramActRow), aliased(TelegramOutboxRow), aliased(TelegramCallbackRow)
        self.callback_column = self.column + 2
        group = select(TelegramCallbackRow.group_id).where(TelegramCallbackRow.id == self.callback_id).correlate(None).scalar_subquery()
        return (
            query.add_columns(
                act,
                parent,
                control,
                select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.activation_id == self.act.id).exists(),
                select(AgentWorkEventRow.id).where(AgentWorkEventRow.operation_id == self.act.id).exists(),
            )
            .join(act, act.id == self.act.id)
            .join(parent, parent.id == self.parent_id)
            .outerjoin(control, control.group_id == group)
        )

    def check_with_permissions(self, rows, held):
        actual_row, parent, _control, attempts, events = rows[0][self.column : self.column + 5]
        actual = _snapshot(actual_row)
        root = _snapshot(rows[0][self.parent.column]) if self.root is not None else None
        conclusive_start(self.link, actual, root)
        if signature(actual, root) != self.signature or attempts is not False or events is not False:
            raise ProductSourceRetired()
        self.parent.check(rows, actual_row if root is not None else actual, held)
        primary_column = self.parent.origin_parent_column if root is not None else self.parent.column
        primary = next((row[primary_column] for row in rows if row[primary_column] is not None), None)
        controls = list({control.id: control for row in rows if (control := row[self.callback_column]) is not None}.values())
        selected = next((control for control in controls if control.id == self.callback_id), None)
        if selected is None:
            raise ProductSourceRetired()
        origin = qualify_execution_origin(self.link, actual, selected, parent, controls, activation_parent=_snapshot(primary) if primary is not None else None, context_root=root)
        if origin["signature"] != self.anchor_signature:
            raise ProductSourceRetired()
