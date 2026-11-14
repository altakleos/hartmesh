"""Original composite consent and receipt in the final activation SQL snapshot."""

from collections.abc import Mapping

from app.telegram_bots.work_continuation_state import continuation_state, require_child
from deerflow.persistence.telegram_bots.model import TelegramActRow


class ActivationParentProjection:
    def __init__(self, child, *, for_effect=True):
        self.child = child
        self.for_effect = for_effect
        self.draft = self.family = self.consent = self.proposal = None
        self.link = self.parent_id = self.root_id = None

    async def prepare(self, acts, link):
        from app.telegram_bots.acts import _snapshot
        from app.telegram_bots.assignment_drafts import DraftProjection, snapshot_draft
        from app.telegram_bots.assignment_lineage import DelegateFamily
        from app.telegram_bots.work_actions import same_body
        from app.telegram_bots.work_delegate_actions import delegate_source, draft_body_signature
        from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

        marker = self.child.continuation.get("activation_parent") if isinstance(self.child.continuation, Mapping) else None
        if not isinstance(marker, Mapping) or type(marker.get("act_id")) is not str:
            return self
        async with acts.session_factory() as reader:
            parent = await reader.get(TelegramActRow, marker["act_id"])
            if parent is None:
                raise ValueError("missing-activation-parent")
            require_child(_snapshot(parent), self.child)
            source = delegate_source(_snapshot(parent))
            root = await reader.get(TelegramActRow, source["draft_id"])
            snapshot_draft(link, root)
            if source["origin_kind"] not in {"internal", "callback"} or draft_body_signature(link, root) != source["draft_signature"]:
                raise ValueError("invalid-activation-consent")
            if root.continuation is not None:
                if source["origin_kind"] != "callback" or not same_body(root.continuation, {"version": 1, "assignment_admission": parent.continuation.get("delegate_origin")}):
                    raise ValueError("invalid-activation-consent")
            elif source["origin_kind"] != "internal":
                raise ValueError("invalid-activation-consent")
            self.link, self.parent_id, self.root_id = link, parent.id, root.id
            self.draft = DraftProjection(link, root.body["origin"], saved=_snapshot(root), require_work_policy=self.for_effect)
            self.family = DelegateFamily(root.id, parent.id)
            if source["origin_kind"] == "callback":
                from app.telegram_bots.assignment_consent import DelegationConsent
                from app.telegram_bots.assignment_proposal_records import ProposalInputs, proposal_primary
                from app.telegram_bots.producer import _item

                self.consent = DelegationConsent(link, _snapshot(parent))
                anchor = await reader.get(TelegramOutboxRow, self.consent.origin["anchor_outbox_id"])
                if anchor is None:
                    raise ValueError("missing-activation-consent")
                self.proposal = ProposalInputs(link, _item(anchor)) if proposal_primary(anchor.payload) else None
        return self

    def extend(self, query):
        from sqlalchemy import String, and_, cast, func, or_
        from sqlalchemy.orm import aliased

        from deerflow.persistence.agent_instances.work import AgentWorkEventRow

        parent = aliased(TelegramActRow)
        self.column = len(query.column_descriptions)
        marker = self.child.continuation.get("activation_parent") if isinstance(self.child.continuation, Mapping) else None
        identity = marker.get("act_id") if isinstance(marker, Mapping) else None
        result = query.add_columns(parent).outerjoin(
            parent,
            or_(
                parent.id == identity,
                parent.continuation["activation_continuation"]["child_id"].as_string() == self.child.id,
                cast(parent.operation_ids, String).like('%"' + self.child.id + '"%'),
            ),
        )
        child, event, original_event = aliased(TelegramActRow), aliased(AgentWorkEventRow), aliased(AgentWorkEventRow)
        self.receipt_column = len(result.column_descriptions)
        result = (
            result.add_columns(child, event, original_event)
            .outerjoin(child, child.id == parent.result["delegate_child_id"].as_string())
            .outerjoin(event, and_(event.instance_id == parent.instance_id, event.operation_id == func.coalesce(parent.result["delegate_child_id"].as_string(), parent.id)))
            .outerjoin(original_event, and_(original_event.instance_id == parent.instance_id, original_event.operation_id == parent.id))
        )
        from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

        attempt = aliased(AgentWorkAttemptRow)
        self.attempt_column = len(result.column_descriptions)
        result = result.add_columns(attempt).outerjoin(attempt, and_(attempt.work_id == self.child.work_id, attempt.activation_id == self.child.id))
        if self.draft is not None:
            result = self.family.extend(self.draft.extend(result))
            if self.consent is not None:
                result = self.consent.extend(result)
            if self.proposal is not None:
                result = self.proposal.extend(result)
            if self.consent is None:
                from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow

                card, control = aliased(TelegramOutboxRow), aliased(TelegramCallbackRow)
                self.reverse_column = len(result.column_descriptions)
                result = (
                    result.add_columns(card, control)
                    .outerjoin(
                        card,
                        and_(
                            card.link_id == self.link.id,
                            card.chat_id == self.link.chat_id,
                            card.binding_generation == self.link.binding_generation,
                            card.owner_user_id == self.link.owner_user_id,
                            card.instance_id == self.link.agent_ref,
                            or_(card.dedupe_key == "notice:assignment:" + self.root_id, card.payload["assignment_intent"]["draft_id"].as_string() == self.root_id),
                        ),
                    )
                    .outerjoin(
                        control,
                        and_(
                            control.link_id == self.link.id,
                            control.chat_id == self.link.chat_id,
                            control.binding_generation == self.link.binding_generation,
                            or_(
                                and_(control.target_kind == "draft", control.target_id == self.root_id),
                                control.payload["draft_id"].as_string() == self.root_id,
                                control.payload["work_action_id"].as_string() == self.parent_id,
                            ),
                        ),
                    )
                )
        return result

    def check(self, rows, actual_child, held):
        from app.telegram_bots.acts import _snapshot
        from app.telegram_bots.work_actions import same_body
        from app.telegram_bots.work_delegate_actions import delegate_source, qualify_delegate_receipt, qualify_no_due

        parents = {value.id: value for row in rows if (value := row[self.column]) is not None}
        marker = actual_child.continuation.get("activation_parent") if isinstance(actual_child.continuation, Mapping) else None
        if not parents and marker is None:
            return
        if len(parents) != 1 or not isinstance(marker, Mapping):
            raise ValueError("invalid-activation-parent")
        parent = next(iter(parents.values()))
        if self.draft is None:
            raise ValueError("missing-activation-consent")
        require_child(_snapshot(parent), _snapshot(actual_child))
        state = continuation_state(_snapshot(parent))
        source = delegate_source(_snapshot(parent))
        from deerflow.agent_instances.work import UNRESOLVED

        promise = rows[0][self.attempt_column]
        exact_promise = (
            promise is not None
            and promise.run_id is None
            and promise.status in UNRESOLVED
            and (promise.work_id, promise.activation_id, promise.requester_id, promise.thread_id) == (actual_child.work_id, actual_child.id, actual_child.owner_user_id, actual_child.body["thread_id"])
            and same_body(promise.request, actual_child.body)
        )
        if (
            parent.state != "done"
            or self.for_effect
            and (parent.cancel_requested_at is not None or not (state["phase"] == "child_planned" or state["phase"] == "refused" and exact_promise))
            or not self.for_effect
            and state["phase"] not in {"child_planned", "done", "refused", "cancelled"}
            or not source["start"]
            or source["parent_id"] is not None
            or source["authorized_at"] != state["authorized_at"]
        ):
            raise ValueError("invalid-activation-parent")
        selected = next(row for row in rows if row[self.column] is not None)
        child, event, original_event = selected[self.receipt_column : self.receipt_column + 3]
        if child is not None:
            qualify_no_due(_snapshot(parent), _snapshot(child))
            receipt = qualify_delegate_receipt(_snapshot(child), event, child.result)
            if original_event is not None or child.state != "done" or not same_body(parent.result, {"delegate_child_id": child.id, "receipt": receipt}):
                raise ValueError("invalid-activation-primary-receipt")
        else:
            receipt = qualify_delegate_receipt(_snapshot(parent), event, parent.result)
        if (receipt["id"], receipt["assignment_revision"]) != (state["work_id"], state["assignment_revision"]):
            raise ValueError("invalid-activation-primary-receipt")
        self.draft.check_with_permissions(rows, held)
        self.family.check(rows, parent)
        root = rows[0][self.draft.column + 2]
        if self.consent is not None:
            self.consent.check(rows, _snapshot(root), _snapshot(parent), _snapshot(child) if child is not None else None)
        elif source["origin_kind"] != "internal" or root.continuation is not None or any(row[self.reverse_column] is not None or row[self.reverse_column + 1] is not None for row in rows):
            raise ValueError("invalid-internal-activation-consent")
        if self.proposal is not None:
            self.proposal.check(rows)
