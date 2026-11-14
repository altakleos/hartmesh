"""Original answer consent and separately admitted Retry remain distinct owners."""

from sqlalchemy import select

from app.telegram_bots.assignment_controls import consent_origin, displayed_assignment
from app.telegram_bots.assignment_drafts import snapshot_draft
from app.telegram_bots.assignment_proposal_records import validate_proposal_reply_basis
from app.telegram_bots.callbacks import _validate_rows
from app.telegram_bots.producer import _item, validate_publication_outcome
from app.telegram_bots.work_actions import same_body
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


async def qualify_answer_owner(session, link, selected, owner):
    """Borrow storage; copied JSON alone cannot authorize a callback profile."""
    from app.telegram_bots.inbox import _callback_owner

    document = owner.get("submission")
    if not isinstance(document, dict) or document.get("version") != 2:
        return
    seen = set()
    row, data = selected, owner
    while True:
        if row.id in seen or (row.link_id, row.chat_id, row.binding_generation, row.message_thread_id) != (link.id, link.chat_id, link.binding_generation, document["parent_publication"]["scope"]["message_thread_id"]):
            raise ValueError("assignment-answer-owner-conflict")
        seen.add(row.id)
        if not same_body(data.get("submission"), document):
            raise ValueError("assignment-answer-owner-conflict")
        if row.id == document["gesture"]["callback_id"]:
            if not same_body(data.get("source_publication"), document["parent_publication"]):
                raise ValueError("assignment-answer-owner-conflict")
            if (row.kind, row.target_kind, row.target_id, row.payload.get("draft_id"), data["source_outbox_id"], data["update_id"]) != (
                "assign_answer",
                "draft",
                document["draft_id"],
                document["draft_id"],
                document["source_outbox_id"],
                document["gesture"]["update_id"],
            ):
                raise ValueError("assignment-answer-owner-conflict")
            parent = await session.get(TelegramOutboxRow, document["source_outbox_id"], populate_existing=True)
            root = await session.get(TelegramActRow, document["draft_id"], populate_existing=True)
            if parent is None or root is None:
                raise ValueError("assignment-answer-owner-conflict")
            original, _source = displayed_assignment(link, parent, row)
            members = list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.group_id)))
            _validate_rows(original, members)
            snapshot_draft(link, root)
            proof = validate_proposal_reply_basis(_item(parent))
            witness = {"version": 1, "assignment_navigation": {"kind": "answer_here", "origin": document["gesture"]["origin"]}}
            if (
                root.state != "refused"
                or root.reason != "answered-here"
                or not same_body(root.result, witness)
                or not same_body(root.body, _source["work"]["root"]["body"])
                or root.operation_ids
                or root.continuation is not None
                or root.stop_dispatch is not None
                or root.cancel_requested_at is not None
                or not same_body(consent_origin(original, row), document["gesture"]["origin"])
                or not same_body(parent.payload["reply_basis"]["outcome"]["proposal_basis"], document["proposal_basis"])
                or proof.record.publication_id != document["parent_publication"]["publication_id"]
                or any(member.consumed_at is None or member.closed_reason != "answered-here" for member in members)
            ):
                raise ValueError("assignment-answer-owner-conflict")
            return
        if row.kind != "retry_message" or row.target_kind != "request" or row.payload.get("anchor_outbox_id") != data["source_outbox_id"]:
            raise ValueError("assignment-answer-owner-conflict")
        parent = await session.get(TelegramOutboxRow, data["source_outbox_id"], populate_existing=True)
        if parent is None or (parent.link_id, parent.owner_user_id, parent.binding_generation, parent.chat_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id):
            raise ValueError("assignment-answer-owner-conflict")
        prior_id = parent.payload.get("input_owner_callback_id")
        receipt = None
        if prior_id is None:
            receipt = validate_publication_outcome(_item(parent))
            prior_id = receipt.record.owner_callback_id
        if type(prior_id) is not str or row.target_id != prior_id:
            raise ValueError("assignment-answer-owner-conflict")
        previous = await session.get(TelegramCallbackRow, prior_id, populate_existing=True)
        if previous is None:
            raise ValueError("assignment-answer-owner-conflict")
        prior, _stage = _callback_owner(previous.payload, prior_id)
        from app.telegram_bots.inbox import publication_identity

        expected_source = publication_identity(receipt.record) if receipt is not None else prior["source_publication"]
        if not same_body(data.get("source_publication"), expected_source):
            raise ValueError("assignment-answer-owner-conflict")
        if not prior["route_complete"] or prior["outbox_id"] != parent.id or parent.id not in prior["delivery_ids"] or prior["input_sha256"] != data["input_sha256"]:
            raise ValueError("assignment-answer-owner-conflict")
        row, data = previous, prior
