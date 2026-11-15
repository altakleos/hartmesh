"""Minimal current permission replies retain actual saved management ownership."""

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.authority import CeilingRefused
from app.telegram_bots.callbacks import _validate_rows
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import same_body, thaw
from app.telegram_bots.work_execution_readiness import readiness_signature
from app.telegram_bots.work_execution_sources import act_facts
from app.telegram_bots.work_management_actions import command_source
from app.telegram_bots.work_management_consent import ManagementModeWitness, management_origin, require_management_scope
from app.telegram_bots.work_management_origins import displayed_management, origin_for
from app.telegram_bots.work_management_sources import WorkManagementProjection, management_intent
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_saved_refusals import SavedStartProof
from app.telegram_bots.work_view_sources import detail_read
from deerflow.agent_instances.contract import AgentDenied
from deerflow.agent_instances.work import UNRESOLVED
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


def refused_command(act):
    source = command_source(act)
    scope = act.stop_dispatch
    return (
        act.state == "refused"
        and act.reason in {"denied", "ceiling", "grant-required", "not-visible", "agent-not-active"}
        and act.attempts >= 0
        and not act.result
        and not act.operation_ids
        and act.cancel_requested_at is None
        and (
            scope is None
            or set(scope) == {"attempt_id", "thread_id", "run_id", "state", "attempts", "not_before"}
            and scope["state"] in {"awaiting_command", "ended"}
            and scope["attempts"] == 0
            and scope["not_before"] is None
            and all(scope[key] == source["scope"][key] for key in ("attempt_id", "thread_id", "run_id"))
        )
    )


def confirmation_signature(intent, origin, draft):
    return digest([intent, origin, act_facts(draft) if draft is not None else None, thaw(draft.result) if draft is not None else None])


class SavedManagementProof(SavedStartProof):
    def __init__(self, owner, link, item, act, draft, origin, control, parent, *, intent):
        if act is not None:
            super().__init__(owner, link, item, act, draft or act, origin, control, parent, None)
        else:
            self.owner, self.link, self.item = owner, link, item
            self.root_id, self.effective_id = control.id, draft.id if draft is not None else control.id
            self.signature = confirmation_signature(intent, origin, draft)
            self.origin, self.group_id, self.parent_id = origin, control.group_id, parent.id
            self.primary_id, self.primary_signature = None, None
            self.metadata = self.captured = self.silent = self.now = None
            self.optional_journal = True
        self.intent, self.original_act = intent, act
        self.management_kind = intent["kind"]

    @classmethod
    async def read(cls, owner, link, item, *, gesture=None):
        intent = management_intent(item)
        if intent["act_id"] is None and gesture is None:
            return None
        if gesture is not None and (
            type(gesture) is not dict
            or set(gesture) != {"callback_id", "query_id"}
            or type(gesture["callback_id"]) is not str
            or len(gesture["callback_id"]) != 32
            or type(gesture["query_id"]) is not str
            or not 1 <= len(gesture["query_id"]) <= 256
            or intent["act_id"] is not None
        ):
            raise ProductSourceRetired()
        async with owner.host.outbox.sessions() as reader:
            identity = intent["act_id"] or gesture["callback_id"]
            row = await reader.get(TelegramActRow, identity)
            if intent["act_id"] is None:
                if row is not None:
                    raise ProductSourceRetired()
                act = None
            else:
                if row is None:
                    raise ProductSourceRetired()
                act = _snapshot(row)
                require_management_scope(link, act, intent)
                if not refused_command(act):
                    return None
            raw_draft = await reader.get(TelegramActRow, intent["draft_id"]) if intent["draft_id"] is not None else None
            if intent["draft_id"] is not None and raw_draft is None:
                raise ProductSourceRetired()
            draft = _snapshot(raw_draft) if raw_draft is not None else None
            control = await reader.get(TelegramCallbackRow, identity)
            parent = await reader.get(TelegramOutboxRow, item.id)
            if control is None or parent is None:
                raise ProductSourceRetired()
            original, source = displayed_management(link, parent, control)
            if control.kind != ("work_change_save" if intent["kind"] == "edit" else "work_withdraw_confirm"):
                raise ProductSourceRetired()
            origin = management_origin(act) if act is not None else origin_for(original, control)
            current = await reader.get(AgentWorkRow, intent["work_id"])
            active = await reader.scalar(select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.work_id == intent["work_id"], AgentWorkAttemptRow.status.in_(UNRESOLVED)))
            if current is None or current.instance_id != link.agent_ref:
                raise ProductSourceRetired()
            required_cancel = command_source(act)["scope"] is not None if act is not None else active is not None
        if act is not None and act.attempts:
            try:
                receipt = await owner.host.acts.receipt_for(act)
            except (AgentDenied, CeilingRefused):
                # Current authority can prevent a receipt read; the final
                # shared-store query must still prove command-event absence.
                pass
            else:
                if receipt is not None:
                    return None
        clock = await owner.chat.management.reader.prepare_clock(link, item)
        proof = cls(owner, link, item, act, draft, origin, control, parent, intent=intent)
        proof.required_routes = frozenset({"agents:write", "runs:cancel"} if required_cancel else {"agents:write"})
        proof.management = WorkManagementProjection(owner.chat.management.reader, link, item, detail=detail_read(source["work"]["detail"]), act=act, clock=clock, origin_only=True)
        return proof

    def extend(self, query):
        query = self.management.extend(super().extend(query))
        if self.original_act is not None:
            self.witness = ManagementModeWitness(self.original_act)
            query = self.witness.extend(query)
        self.current_column = len(query.column_descriptions)
        work, attempt = aliased(AgentWorkRow), aliased(AgentWorkAttemptRow)
        query = query.add_columns(work, attempt).join(work, and_(work.id == self.intent["work_id"], work.instance_id == self.link.agent_ref)).outerjoin(attempt, and_(attempt.work_id == work.id, attempt.status.in_(UNRESOLVED)))
        self.group_owned_column = len(query.column_descriptions)
        members = select(TelegramCallbackRow.id).where(TelegramCallbackRow.group_id == self.group_id)
        return query.add_columns(or_(select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.activation_id.in_(members)).exists(), select(AgentWorkEventRow.id).where(AgentWorkEventRow.operation_id.in_(members)).exists()))

    def check_source(self, rows, root, effective, canonical, parent, primary, native, event):
        if native is not False or event is not False or any(row[self.group_owned_column] is not False for row in rows) or not same_body(canonical.payload.get("work_management_intent"), self.item.payload.get("work_management_intent")):
            raise ProductSourceRetired()
        if self.original_act is not None:
            if root is None:
                raise ProductSourceRetired()
            require_management_scope(self.link, root, self.intent)
            if not refused_command(root) or readiness_signature(root, effective) != self.signature:
                raise ProductSourceRetired()
            self.witness.check(rows)
        else:
            draft = effective if self.intent["draft_id"] is not None else None
            if root is not None or self.intent["draft_id"] is None and effective is not None or confirmation_signature(self.intent, self.origin, draft) != self.signature:
                raise ProductSourceRetired()
            controls = list({row[self.column + 4].id: row[self.column + 4] for row in rows if row[self.column + 4] is not None}.values())
            selected = next((row for row in controls if row.id == self.root_id), None)
            if selected is None:
                raise ProductSourceRetired()
            original, _source = displayed_management(self.link, canonical, selected)
            _validate_rows(original, controls)
            self.owner.host.callbacks._unexpired(selected)
            work, _attempt = rows[0][self.current_column : self.current_column + 2]
            routes = frozenset({"agents:write", "runs:cancel"} if any(row[self.current_column + 1] is not None for row in rows) else {"agents:write"})
            if (
                origin_for(original, selected) != self.origin
                or any(control.consumed_at is not None or control.payload.get("work_action_id") is not None for control in controls)
                or (work.revision, work.assignment_revision) != (self.intent["expected_revision"], self.intent["expected_assignment_revision"])
                or routes != self.required_routes
            ):
                raise ProductSourceRetired()
        self.management.check_ownership(rows)
