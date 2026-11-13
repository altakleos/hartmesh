"""Own-draft observers retain exact custody through receipt settlement."""

from sqlalchemy import false
from sqlalchemy.orm import aliased

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.assignment_consent import DelegationConsent
from app.telegram_bots.assignment_drafts import DraftProjection
from app.telegram_bots.assignment_lineage import DelegateFamily
from app.telegram_bots.assignment_sources import private_facts, private_snapshot
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import _ACT_SCOPE, same_body
from deerflow.persistence.telegram_bots.model import TelegramActRow


async def prepare_receipt_guard(reader, link, source):
    if source.key != "assignment.card" or source.work.get("act") is None:
        raise ProductSourceRetired()
    projection = ReceiptProjection(link, source)
    current = await prepare_attention(reader, link)

    async def admit(writer, _completed):
        await current(session=writer, projection=projection)

    return admit


class ReceiptProjection:
    def __init__(self, link, source):
        self.link = link
        self.root, self.act = private_snapshot(source.work["root"]), private_snapshot(source.work["act"])
        self.child = private_snapshot(source.work["child"]) if source.work.get("child") is not None else None
        self.draft = DraftProjection(link, self.root.body["origin"], saved=self.root)
        self.consent = DelegationConsent(link, self.act)
        self.family = DelegateFamily(self.root.id, self.act.id)

    def extend(self, query):
        result = self.draft.extend(query)
        self.column = len(result.column_descriptions)
        act, child = aliased(TelegramActRow), aliased(TelegramActRow)
        result = result.add_columns(act, child).join(act, act.id == self.act.id).outerjoin(child, child.id == self.child.id if self.child is not None else false())
        return self.consent.extend(self.family.extend(result))

    def check_with_permissions(self, rows, held):
        self.draft.check_with_permissions(rows, held)
        root = rows[0][self.draft.column + 2]
        if not same_body(private_facts(root), private_facts(self.root)):
            raise ProductSourceRetired()
        act, child = rows[0][self.column : self.column + 2]
        self.family.check(rows, act)
        pairs = [(act, self.act), *([(child, self.child)] if self.child is not None else [])]
        for actual, expected in pairs:
            if (
                actual is None
                or any(getattr(actual, key) != getattr(expected, key) for key in _ACT_SCOPE)
                or not same_body(actual.body, expected.body)
                or not same_body(actual.operation_ids, expected.operation_ids)
                or not same_body(actual.continuation.get("delegate_source"), expected.continuation.get("delegate_source"))
                or not same_body(actual.continuation.get("delegate_origin"), expected.continuation.get("delegate_origin"))
            ):
                raise ProductSourceRetired()
        self.consent.check(rows, root, _snapshot(act), _snapshot(child) if child is not None else None)
