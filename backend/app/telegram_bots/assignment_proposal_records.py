"""Closed publication custody for a source-qualified private proposal card."""

from types import SimpleNamespace

from app.telegram_bots.assignment_drafts import draft_identity, origin_facts, snapshot_draft
from app.telegram_bots.assignment_proposal_source import validate_proposal_basis
from app.telegram_bots.assignment_sources import private_snapshot
from app.telegram_bots.inbox import PublicationOrigin, PublicationRecord, PublicationScope, validate_publication_inputs
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import same_body


def proposal_primary(payload):
    basis = payload.get("reply_basis")
    return type(basis) is dict and type(basis.get("version")) is int and basis["version"] == 9


def original_news(item, link=None):
    from app.telegram_bots.work_news_delivery import frozen_news

    frozen_news(SimpleNamespace(item=item, link=link, part=None))
    current = item.payload["work_news_basis"]
    return current["history"][0]["basis"] if current["version"] == 3 else current


def validate_proposal_reply_basis(item, part=None):
    """Validate frozen intent and rendering; live readers still own admission."""
    from app.gateway.run_output import RunOutputProjectionV2
    from app.telegram_bots.model_source import VerifiedModelReplyBasis

    try:
        basis = item.payload["reply_basis"]
        if (
            type(basis) is not dict
            or set(basis) != {"version", "publication", "outcome", "presentation", "parts", "finish_route", "inputs", "draft_id"}
            or type(basis["version"]) is not int
            or basis["version"] != 9
            or basis["finish_route"] is not True
            or basis["presentation"] is not None
            or {"product_basis", "delivery_kind", "turn_outcome_basis"}.intersection(item.payload)
        ):
            raise ValueError()
        outcome = basis["outcome"]
        if type(outcome) is not dict or set(outcome) != {"kind", "proposal_basis"} or outcome["kind"] != "assignment_proposal":
            raise ValueError()
        source_basis = outcome["proposal_basis"]
        output = RunOutputProjectionV2.model_validate(source_basis["projection"])
        identity = basis["publication"]
        scope = PublicationScope(**identity["scope"])
        record = PublicationRecord(**{**identity, "scope": scope, "origins": tuple(PublicationOrigin(**origin) for origin in identity["origins"]), "dispatch_started": True, "run_id": output.output.run_id})
        proposed = validate_proposal_basis(record, source_basis)
        inputs = validate_publication_inputs(record, basis["inputs"])
        first = min(inputs, key=lambda value: (value.accepted_at, value.update_id, value.message_id))
        link = SimpleNamespace(**{**vars(scope), "id": scope.link_id})
        news = original_news(item, link)
        source = news["source"]
        root = private_snapshot(source["work"]["root"])
        snapshot = snapshot_draft(link, root)
        if (
            (item.link_id, item.owner_user_id, item.binding_generation, item.chat_id, item.instance_id, item.dedupe_key, item.class_)
            != (scope.link_id, scope.owner_user_id, scope.binding_generation, scope.chat_id, scope.agent_ref, "reply:" + record.turn_id, "reply")
            or item.message_thread_id not in (scope.message_thread_id, None)
            or not same_body(item.payload.get("source"), outcome)
            or not same_body(source_basis["publication"], identity)
            or (source["key"], source["work"]["phase"], snapshot.source, snapshot.state) != ("assignment.card", "draft", "tool", "draft")
            or root.operation_ids
            or root.continuation is not None
            or snapshot.id != basis["draft_id"]
            or snapshot.id != draft_identity(link, first)
            or source["work"]["draft_id"] != snapshot.id
            or not same_body(item.payload.get("assignment_intent"), {"version": 1, "draft_id": snapshot.id})
            or not same_body(root.body["origin"], origin_facts(first))
            or not same_body(snapshot.original.model_dump(mode="json"), proposed.model_dump(mode="json"))
            or not same_body(news["parts"], basis["parts"])
        ):
            raise ValueError()
        if part is not None:
            from app.telegram_bots.work_news_delivery import frozen_news

            frozen_news(SimpleNamespace(item=item, link=link, part=part), pending=part.get("state") == "pending", acknowledged=part.get("state") == "sent")
        return VerifiedModelReplyBasis(record, output, None, "assignment_proposal")
    except Exception:
        raise ValueError("assignment-proposal-reply-conflict") from None


def assignment_key(root, payload=None):
    """One canonical parent, whether from explicit words or a publication."""
    if payload is not None and proposal_primary(payload):
        basis = payload["reply_basis"]
        if basis.get("draft_id") != root.id or root.body["source"] != "tool":
            raise ProductSourceRetired()
        return "reply:" + basis["publication"]["turn_id"]
    return "notice:assignment:" + root.id


async def assignment_parent(reader, link, draft_id):
    from sqlalchemy import or_, select

    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    values = list(
        await reader.scalars(
            select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, or_(TelegramOutboxRow.dedupe_key == "notice:assignment:" + draft_id, TelegramOutboxRow.payload["assignment_intent"]["draft_id"].as_string() == draft_id))
        )
    )
    if len(values) > 1:
        raise ProductSourceRetired()
    return values[0] if values else None


class ProposalInputs:
    """Retain the actual original input set in the final joined SQL snapshot."""

    def __init__(self, link, item):
        self.link, self.item = link, item
        self.record = validate_proposal_reply_basis(item).record
        self.expected = validate_publication_inputs(self.record, item.payload["reply_basis"]["inputs"])
        self.root = private_snapshot(original_news(item, link)["source"]["work"]["root"])

    def extend(self, query):
        from sqlalchemy import and_, or_
        from sqlalchemy.orm import aliased

        from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow

        root = aliased(TelegramActRow)
        mark = aliased(TelegramInboundMarkRow)
        self.root_column = len(query.column_descriptions)
        result = query.add_columns(root).outerjoin(root, root.id == self.root.id)
        self.column = len(result.column_descriptions)
        return result.add_columns(mark).outerjoin(mark, or_(*[and_(mark.link_id == value.link_id, mark.message_id == value.message_id, mark.chat_id == value.chat_id) for value in self.expected]))

    def check(self, rows):
        from app.telegram_bots.assignment_drafts import draft_scope
        from app.telegram_bots.inbox import InboxRow, _publication_input, _submission_input

        root = rows[0][self.root_column] if rows else None
        if root is None or any(getattr(root, key) != value for key, value in draft_scope(self.link).items()) or not same_body(root.body, self.root.body):
            raise ProductSourceRetired()
        actual = {value.message_id: InboxRow.from_row(value) for row in rows if (value := row[self.column]) is not None}
        values = tuple(actual.values())
        submission = self.item.payload["reply_basis"]["inputs"]["submission"]
        signature = _publication_input(values) if submission is None else _submission_input(values, submission)
        if (
            set(actual) != {value.message_id for value in self.expected}
            or signature != self.record.input_sha256
            or any(value.state == "interrupted" or self.item.id not in value.delivery_ids or value.outbox_id != self.item.id for value in values)
        ):
            raise ProductSourceRetired()
