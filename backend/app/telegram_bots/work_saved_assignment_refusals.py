"""Fixed metadata for an actual admitted delegation that made no service call."""

from sqlalchemy import false, or_, select

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.assignment_consent import DelegationConsent, delegate_origin
from app.telegram_bots.assignment_drafts import snapshot_draft
from app.telegram_bots.assignment_lineage import DelegateFamily, read_family
from app.telegram_bots.assignment_sources import assignment_intent
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import same_body, thaw
from app.telegram_bots.work_continuation_state import continuation_state
from app.telegram_bots.work_delegate_actions import delegate_source, require_delegate_scope
from app.telegram_bots.work_execution_sources import act_facts
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_saved_refusals import SavedStartProof
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow


def unattempted_delegation(link, act):
    require_delegate_scope(link, act)
    source = delegate_source(act)
    if source["start"]:
        state = continuation_state(act)
        if state["phase"] not in {"awaiting_primary", "refused"} or any(state[key] is not None for key in ("work_id", "assignment_revision", "child_body")):
            return False
    elif (act.continuation or {}).get("activation_continuation") is not None:
        return False
    return (
        act.kind == "delegate"
        and act.state == "refused"
        and act.reason in {"denied", "ceiling", "grant-required", "not-visible", "agent-not-active"}
        and act.attempts == 0
        and not act.operation_ids
        and act.work_id is None
        and act.cancel_requested_at is None
        and act.stop_dispatch is None
        and not act.result
        and source["parent_id"] is None
        and source["origin_kind"] == "callback"
    )


def refusal_signature(act, draft):
    facts = act_facts(act)
    if delegate_source(act)["start"]:
        # The existing continuation owner records this refused primary without
        # authorizing a child. Only that terminal bookkeeping is equivalent.
        facts["continuation"]["activation_continuation"]["phase"] = "refused"
    return digest([facts, thaw(act.result), act_facts(draft), thaw(draft.result)])


class SavedAssignmentProof(SavedStartProof):
    assignment = True

    @classmethod
    async def read(cls, owner, link, item):
        intent = assignment_intent(item)
        async with owner.host.outbox.sessions() as reader:
            draft = await reader.get(TelegramActRow, intent["draft_id"])
            snapshot_draft(link, draft)
            if len(draft.operation_ids) != 1:
                return None
            row = await reader.get(TelegramActRow, draft.operation_ids[0])
            if row is None:
                return None
            act = _snapshot(row)
            if not unattempted_delegation(link, act):
                return None
            await read_family(reader, draft.id, act)
            origin = delegate_origin(act)
            consent = DelegationConsent(link, act)
            facts = (await reader.execute(consent.extend(select(TelegramActRow).select_from(TelegramActRow).where(TelegramActRow.id == draft.id)))).all()
            if not facts:
                raise ProductSourceRetired()
            control, parent = facts[0][consent.column : consent.column + 2]
            consent.check(facts, draft, row)
            return cls(owner, link, item, act, _snapshot(draft), origin, control, parent, None)

    def extend(self, query):
        query = super().extend(query)
        self.consent = DelegationConsent(self.link, self.original_act)
        self.family = DelegateFamily(self.effective_id, self.root_id)
        query = self.family.extend(self.consent.extend(query))
        self.child_column = len(query.column_descriptions)
        child_owned = (
            or_(
                select(TelegramActRow.id).where(TelegramActRow.id == self.child_id).exists(),
                select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.activation_id == self.child_id).exists(),
                select(AgentWorkEventRow.id).where(AgentWorkEventRow.operation_id == self.child_id).exists(),
            )
            if self.child_id is not None
            else false()
        )
        return query.add_columns(child_owned)

    def __init__(self, owner, link, item, root, effective, origin, callback, parent, primary):
        super().__init__(owner, link, item, root, effective, origin, callback, parent, primary)
        self.original_act = root
        self.signature = refusal_signature(root, effective)
        self.child_id = continuation_state(root)["child_id"] if delegate_source(root)["start"] else None

    def check_source(self, rows, root, effective, canonical, parent, primary, native, event):
        if not unattempted_delegation(self.link, root) or refusal_signature(root, effective) != self.signature or native is not False or event is not False or any(row[self.child_column] is not False for row in rows):
            raise ProductSourceRetired()
        draft, act = rows[0][self.column + 1], rows[0][self.column]
        snapshot_draft(self.link, draft)
        self.family.check(rows, root)
        self.consent.check(rows, draft, act)
        if (
            (canonical.link_id, canonical.owner_user_id, canonical.binding_generation, canonical.chat_id, canonical.instance_id, canonical.work_id, canonical.message_thread_id)
            != (self.link.id, self.link.owner_user_id, self.link.binding_generation, self.link.chat_id, self.link.agent_ref, None, self.item.message_thread_id)
            or canonical.dedupe_key != "notice:assignment:" + draft.id
            or not same_body(canonical.payload.get("assignment_intent"), self.item.payload.get("assignment_intent"))
            or assignment_intent(self.item)["draft_id"] != draft.id
        ):
            raise ProductSourceRetired()
