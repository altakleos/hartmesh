"""Current Start denials retain an actual successful composite primary."""

from datetime import timedelta

from sqlalchemy import and_, false
from sqlalchemy.orm import aliased

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.assignment_consent import DelegationConsent, delegate_origin
from app.telegram_bots.assignment_drafts import snapshot_draft
from app.telegram_bots.assignment_lineage import DelegateFamily, read_family
from app.telegram_bots.assignment_sources import assignment_intent
from app.telegram_bots.authority import AccessOff, CeilingRefused
from app.telegram_bots.producer import DeliveryRetry, ProductSourceRetired
from app.telegram_bots.work_actions import _ACT_SCOPE, same_body, thaw
from app.telegram_bots.work_continuation_state import child_namespace_owned, continuation_state
from app.telegram_bots.work_delegate_actions import delegate_source, delegation_children, qualify_delegate_receipt, qualify_no_due, require_delegate_scope
from app.telegram_bots.work_details import WorkDetailProjection
from app.telegram_bots.work_execution_readiness import current_execution_failure
from app.telegram_bots.work_execution_sources import act_facts
from app.telegram_bots.work_request_continuation_sources import RequestPrimaryProof
from app.telegram_bots.work_request_continuations import request_activation
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_saved_refusals import _UNAVAILABLE, SavedStartProof
from app.telegram_bots.work_views import work_start_permissions
from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.agent_instances.work import AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInstancePolicyRow, TelegramOutboxRow


def unadmitted_child(parent):
    state = continuation_state(parent)
    children = list(delegation_children(parent)) if parent.kind == "delegate" else []
    return (
        parent.state == "done"
        and parent.cancel_requested_at is None
        and parent.stop_dispatch is None
        and parent.result is not None
        and list(parent.operation_ids) == children
        and state["phase"] in {"ready", "refused", "expired"}
        and state["child_body"] is None
        and state["work_id"] is not None
    )


def source_signature(parent, effective):
    facts = act_facts(parent)
    # Equivalent bookkeeping for an absent child never changes the original
    # primary receipt, explicit consent, target or deterministic operation ID.
    state = facts["continuation"]["activation_continuation"]
    state["phase"] = "unadmitted"
    state.pop("refusal", None)
    if parent.kind in {"decide", "respond"}:
        # Recording the separately qualified primary ACK cannot replace an
        # already planned current denial or change the original effect consent.
        facts["continuation"].pop("work_outcome", None)
    return digest([facts, thaw(parent.result), act_facts(effective) if effective.id != parent.id else None])


class SavedCompositeProof(SavedStartProof):
    composite = True

    @classmethod
    async def read(cls, owner, link, item):
        async with owner.host.outbox.sessions() as reader:
            if "assignment_intent" in item.payload:
                draft = await reader.get(TelegramActRow, assignment_intent(item)["draft_id"])
                snapshot_draft(link, draft)
                if len(draft.operation_ids) != 1:
                    return None
                row = await reader.get(TelegramActRow, draft.operation_ids[0])
            else:
                identity = item.payload.get("work_outcome_basis", {}).get("context", {}).get("act", {}).get("id")
                row = await reader.get(TelegramActRow, identity) if type(identity) is str else None
                draft = row
            if row is None or row.kind not in {"delegate", "decide", "respond"} or not (row.continuation or {}).get("activation_continuation"):
                return None
            parent = _snapshot(row)
            if not unadmitted_child(parent):
                return None
            if parent.kind == "delegate":
                require_delegate_scope(link, parent)
                source = delegate_source(parent)
                if source["origin_kind"] != "callback" or source["parent_id"] is not None or source["start"] is not True:
                    return None
                await read_family(reader, draft.id, parent)
                origin = delegate_origin(parent)
            else:
                origin = request_activation(parent)["origin"]
            control = await reader.get(TelegramCallbackRow, origin["callback_id"])
            anchor = await reader.get(TelegramOutboxRow, origin["anchor_outbox_id"])
            if control is None or anchor is None:
                raise ProductSourceRetired()
            proof = cls(owner, link, item, parent, _snapshot(draft), origin, control, anchor, None)
        try:
            receipt = await owner.host.acts.receipt_for(parent)
        except (AgentDenied, CeilingRefused, AccessOff):
            pass  # The final SQL statement must qualify the real primary.
        else:
            if receipt is None:
                raise ProductSourceRetired()
        try:
            view = await owner.host.authority.instance_view(link)
        except (AgentDenied, CeilingRefused, AccessOff):
            view = None
        if view is not None and view.status == "active" and view.permissions & 3 == 3:
            try:
                detail = await owner.chat.views.reader.detail(link, proof.state["work_id"])
            except (AgentDenied, CeilingRefused, AccessOff):
                pass
            else:
                proof.required_routes = frozenset({"agents:read", *work_start_permissions(detail.record, can_reuse_conversation=detail.can_reuse_conversation)})
                proof.detail = WorkDetailProjection(owner.chat.views.reader, link, detail.record, detail.request, reusable=detail.can_reuse_conversation, expected=detail)
                proof.work_enabled = detail.record["work_enabled"]
                if proof.work_enabled and proof.required_routes.issubset(detail.held):
                    proof.execution_failure = await current_execution_failure(owner.chat.views.reader, link)
                    proof.readiness_checked = True
        return proof

    def __init__(self, owner, link, item, root, effective, origin, callback, parent, primary):
        super().__init__(owner, link, item, root, effective, origin, callback, parent, primary)
        self.original, self.state = root, continuation_state(root)
        self.signature = source_signature(root, effective)
        self.required_routes = frozenset({"agents:read", "agents:write", "runs:create"})
        self.detail = None
        self.work_enabled, self.execution_failure = None, None
        self.readiness_checked = False
        self.consent = DelegationConsent(link, root) if root.kind == "delegate" else None
        self.family = DelegateFamily(effective.id, root.id) if self.consent is not None else None
        self.request = RequestPrimaryProof(owner.host.acts, link, root) if self.consent is None else None

    def metadata_options(self):
        return {**super().metadata_options(), "work_enabled": self.work_enabled, "execution_failure": self.execution_failure}

    def check_primary_ack(self, canonical, root):
        if self.captured is None:
            return  # A durable plan may wait for its actual primary delivery.
        if canonical.state in {"pending", "sending"}:
            raise DeliveryRetry(timedelta(seconds=1))
        if canonical.state != "sent":
            raise ProductSourceRetired()
        if self.consent is None:
            from app.telegram_bots.work_outcome_delivery import acknowledged_parts

            basis = canonical.payload["work_outcome_basis"]
            acknowledged_parts(canonical, basis["parts"], edit=basis.get("edit") is not None)
        else:
            from app.telegram_bots.assignment_journals import created_work
            from app.telegram_bots.assignment_sources import private_snapshot
            from app.telegram_bots.work_view_origins import acknowledged_view

            shown = acknowledged_view(self.link, canonical, canonical.payload["presentation_version"])
            source = shown.payload["work_news_basis"]["source"]
            data = source["work"]
            saved = private_snapshot(data["act"])
            if (
                source["key"] != "assignment.card"
                or data["phase"] != "saved"
                or saved.id != root.id
                or any(getattr(saved, key) != getattr(root, key) for key in _ACT_SCOPE)
                or saved.kind != root.kind
                or saved.state != "done"
                or saved.attempts != root.attempts
                or not same_body(saved.body, root.body)
                or not same_body(saved.result, root.result)
                or created_work(source, self.link) != self.state["work_id"]
            ):
                raise ProductSourceRetired()

    async def recheck_readiness(self, metadata):
        # Route/settings preparation may await after the initial read. Repeat
        # the actual read-only probe before the final SQL ownership fence.
        if metadata.key in _UNAVAILABLE and self.readiness_checked and await current_execution_failure(self.owner.chat.views.reader, self.link) != self.execution_failure:
            raise ProductSourceRetired()

    def extend(self, query):
        query = super().extend(query)
        parent, child, event, child_event, work, policy = aliased(TelegramActRow), aliased(TelegramActRow), aliased(AgentWorkEventRow), aliased(AgentWorkEventRow), aliased(AgentWorkRow), aliased(TelegramInstancePolicyRow)
        self.primary_column = len(query.column_descriptions)
        ids = list(delegation_children(self.original)) if self.consent is not None else []
        query = (
            query.add_columns(parent, child, event, child_event, work, policy.policy)
            .join(parent, parent.id == self.root_id)
            .outerjoin(child, child.id == ids[0] if ids else false())
            .outerjoin(event, and_(event.instance_id == self.link.agent_ref, event.operation_id == parent.id))
            .outerjoin(child_event, and_(child_event.instance_id == self.link.agent_ref, child_event.operation_id == child.id))
            .join(work, and_(work.id == self.state["work_id"], work.instance_id == self.link.agent_ref))
            .outerjoin(policy, policy.instance_id == self.link.agent_ref)
        )
        self.absence_column = len(query.column_descriptions)
        query = query.add_columns(child_namespace_owned(self.root_id, self.state["child_id"]))
        query = self.family.extend(self.consent.extend(query)) if self.consent is not None else self.request.extend(query, parent)
        return self.detail.extend(query, instance_entity=self.instance_entity) if self.detail is not None else query

    def check_source(self, rows, root, effective, canonical, parent, primary, native, event):
        if not unadmitted_child(root) or source_signature(root, effective) != self.signature or native is not False or any(row[self.absence_column] is not False for row in rows):
            raise ProductSourceRetired()
        actual, child, receipt, child_receipt, work, policy = rows[0][self.primary_column : self.primary_column + 6]
        state = continuation_state(root)
        if work.assignment_revision != state["assignment_revision"]:
            raise ProductSourceRetired()
        if self.consent is not None:
            snapshot_draft(self.link, effective)
            self.family.check(rows, root)
            self.consent.check(rows, effective, root, _snapshot(child) if child is not None else None)
            if child is None:
                saved = qualify_delegate_receipt(root, receipt, root.result)
            else:
                qualify_no_due(root, _snapshot(child))
                saved = qualify_delegate_receipt(_snapshot(child), child_receipt, child.result)
                if receipt is not None or child.state != "done" or not same_body(root.result, {"delegate_child_id": child.id, "receipt": saved}):
                    raise ProductSourceRetired()
            if (saved["id"], saved["assignment_revision"]) != (state["work_id"], state["assignment_revision"]):
                raise ProductSourceRetired()
            if assignment_intent(self.item)["draft_id"] != self.effective_id or not same_body(canonical.payload.get("assignment_intent"), self.item.payload["assignment_intent"]):
                raise ProductSourceRetired()
        else:
            self.request.check_primary(rows, root)
            if canonical.id != self.request.source["outcome_id"] or not same_body(canonical.payload.get("work_outcome_basis"), self.item.payload["work_outcome_basis"]):
                raise ProductSourceRetired()
            if "work_outcome" in root.continuation:
                from app.telegram_bots.work_outcome_delivery import delivery_receipt, phase

                expected = {"phase": phase(canonical.payload["work_outcome_basis"]["context"]["act"]), "outbox_id": canonical.id}
                if not same_body(delivery_receipt(root.continuation), expected):
                    raise ProductSourceRetired()
        if (canonical.link_id, canonical.owner_user_id, canonical.binding_generation, canonical.chat_id, canonical.message_thread_id, canonical.class_) != (
            self.link.id,
            self.link.owner_user_id,
            self.link.binding_generation,
            self.link.chat_id,
            self.item.message_thread_id,
            "reply",
        ):
            raise ProductSourceRetired()
        self.check_primary_ack(canonical, root)
        if self.detail is not None:
            self.detail.check_with_context(rows, self.metadata.decision.held, instance=rows[0][self.column + 6], permissions=rows[0][self.column + 12], policy=policy)
