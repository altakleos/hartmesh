"""An edit retains the original full card and its distinct correction input."""

from datetime import timedelta

from sqlalchemy import and_
from sqlalchemy.orm import aliased

from app.telegram_bots.assignment_controls import consent_origin, displayed_assignment
from app.telegram_bots.assignment_drafts import DraftProjection, origin_facts, snapshot_draft
from app.telegram_bots.assignment_lineage import FreshDraft
from app.telegram_bots.assignment_sources import private_facts
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import _ACT_SCOPE, same_body
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


def replacement_witness(identity, incoming):
    return {"version": 1, "assignment_replacement": {"draft_id": identity, "input": incoming}}


class EditProof:
    def __init__(self, link, origin, *, clock, config):
        self.link, self.origin, self.clock, self.config = link, origin, clock, config

    def extend(self, query):
        self.column = len(query.column_descriptions)
        control, card, member = aliased(TelegramCallbackRow), aliased(TelegramOutboxRow), aliased(TelegramCallbackRow)
        return query.add_columns(control, card, member).join(control, control.id == self.origin["callback_id"]).join(card, card.id == self.origin["anchor_outbox_id"]).outerjoin(member, member.group_id == control.group_id)

    def check(self, rows, root, mark, *, replacement=None):
        control, card = rows[0][self.column : self.column + 2]
        members = list({row[self.column + 2].id: row[self.column + 2] for row in rows if row[self.column + 2] is not None}.values())
        try:
            original, source = displayed_assignment(self.link, card, control)
            _validate_rows(original, members)
            frozen = source["work"]["root"]
            if (
                control.kind != "assign_edit"
                or consent_origin(original, control) != self.origin
                or root.id != control.target_id
                or card.id not in mark.delivery_ids
                or mark.state == "interrupted"
                or any(getattr(root, key) != frozen[key] for key in _ACT_SCOPE)
                or not same_body(root.body, frozen["body"])
                or root.operation_ids
                or root.continuation is not None
                or root.stop_dispatch is not None
                or root.cancel_requested_at is not None
                or any(member.payload.get("work_action_id") is not None for member in members)
            ):
                raise ValueError()
            facts = private_facts(root)
            if replacement is not None and root.state == "refused" and root.reason == "replaced" and same_body(root.result, replacement):
                if any(member.consumed_at is None or member.closed_reason != "replaced" for member in members) or not same_body(
                    {key: value for key, value in facts.items() if key not in {"state", "reason", "updated_at", "result"}}, {key: value for key, value in frozen.items() if key not in {"state", "reason", "updated_at", "result"}}
                ):
                    raise ValueError()
            elif (
                root.state != "draft"
                or not same_body(facts, frozen)
                or any(member.consumed_at is not None for member in members)
                or self.clock() >= aware(root.created_at) + timedelta(hours=self.config.callbacks.draft_ttl_hours)
                or any(member.expires_at is not None and self.clock() >= aware(member.expires_at) for member in members)
            ):
                raise ValueError()
        except (ValueError, KeyError, TypeError, AttributeError, CallbackRefused):
            raise ProductSourceRetired() from None
        return members


class EditFrame:
    def __init__(self, link, original, origin, *, clock, config):
        self.facts = DraftProjection(link, original.body["origin"], saved=original)
        self.proof = EditProof(link, origin, clock=clock, config=config)
        self.fresh = FreshDraft(original.id, origin["anchor_outbox_id"])

    def extend(self, query):
        return self.fresh.extend(self.proof.extend(self.facts.extend(query)))

    def check_with_permissions(self, rows, held):
        self.facts.check_with_permissions(rows, held)
        self.proof.check(rows, rows[0][self.facts.column + 2], rows[0][self.facts.column + 1])
        self.fresh.check(rows, rows[0][self.proof.column + 1])


class EditAdmissionProjection:
    def __init__(self, link, original, origin, incoming, identity, *, clock, config):
        self.link, self.original, self.incoming, self.identity = link, original, incoming, identity
        self.facts = DraftProjection(link, original.body["origin"], saved=original)
        self.proof = EditProof(link, origin, clock=clock, config=config)
        self.fresh = FreshDraft(original.id, origin["anchor_outbox_id"])
        self.witness = replacement_witness(identity, incoming)

    def extend(self, query):
        result = self.proof.extend(self.facts.extend(query))
        self.column = len(result.column_descriptions)
        mark, target = aliased(TelegramInboundMarkRow), aliased(TelegramActRow)
        result = (
            result.add_columns(mark, target).join(mark, and_(mark.link_id == self.incoming["link_id"], mark.chat_id == self.incoming["chat_id"], mark.message_id == self.incoming["message_id"])).outerjoin(target, target.id == self.identity)
        )
        return self.fresh.extend(result)

    def check_with_permissions(self, rows, held):
        self.facts.check_with_permissions(rows, held)
        original, mark = rows[0][self.facts.column + 2], rows[0][self.facts.column + 1]
        incoming, target = rows[0][self.column : self.column + 2]
        if incoming.state == "interrupted" or not same_body(origin_facts(InboxRow.from_row(incoming)), self.incoming):
            raise ProductSourceRetired()
        self.proof.check(rows, original, mark, replacement=self.witness)
        if original.state == "refused":
            if target is None:
                raise ProductSourceRetired()
            draft = snapshot_draft(self.link, target)
            if draft.source != "edit" or not same_body(target.body["origin"], self.incoming):
                raise ProductSourceRetired()
        self.fresh.check(rows, rows[0][self.proof.column + 1])

    def check(self, rows):
        raise ProductSourceRetired()


async def admit_edit(writer, link, source, identity, incoming, current, acts, *, observing=False):
    from app.telegram_bots.acts import _snapshot

    original_id, origin = source.work["intent"]["draft_id"], source.work["intent"]["edit_origin"]
    original = await writer.get(TelegramActRow, original_id, with_for_update=True, populate_existing=True)
    target = await writer.get(TelegramActRow, identity, populate_existing=True)
    if original is None:
        raise ProductSourceRetired()
    witness = replacement_witness(identity, incoming)
    if target is not None and target.body.get("version") == 1:
        replacement = snapshot_draft(link, target)
        if replacement.source != "edit" or not same_body(target.body["origin"], incoming):
            raise ProductSourceRetired()
        if original.state == "draft":
            if observing or target.operation_ids or target.continuation is not None:
                raise ProductSourceRetired()
            frame = EditFrame(link, _snapshot(original), origin, clock=acts.outbox.clock, config=acts.outbox.config)
            await current(session=writer, projection=frame)
            if not same_body(private_facts(original), source.work["root"]):
                raise ProductSourceRetired()
            original.state, original.reason, original.result, original.updated_at = "refused", "replaced", witness, acts.outbox.clock()
            control = await writer.get(TelegramCallbackRow, origin["callback_id"], populate_existing=True)
            from sqlalchemy import select

            for member in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == control.group_id).with_for_update()):
                member.consumed_at, member.closed_reason = acts.outbox.clock(), "replaced"
            await writer.flush()
        await current(session=writer, projection=EditAdmissionProjection(link, _snapshot(original), origin, incoming, identity, clock=acts.outbox.clock, config=acts.outbox.config))
    else:
        frame = EditFrame(link, _snapshot(original), origin, clock=acts.outbox.clock, config=acts.outbox.config)
        await current(session=writer, projection=frame)
        if not same_body(private_facts(original), source.work["root"]):
            raise ProductSourceRetired()
