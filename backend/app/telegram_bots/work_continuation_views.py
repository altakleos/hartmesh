"""A settled primary receipt owns a separate current assignment preview."""

from sqlalchemy import select
from sqlalchemy.orm import aliased

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.authority import AccessOff, CeilingRefused
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.sql_readers import reader_session
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_continuation_state import child_namespace_owned, continuation_state
from app.telegram_bots.work_outcome_delivery import acknowledged_parts, delivery_receipt, phase
from app.telegram_bots.work_request_continuation_sources import RequestPrimaryProof
from app.telegram_bots.work_request_continuations import request_activation
from app.telegram_bots.work_saved_composite_refusals import source_signature, unadmitted_child
from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def is_continuation_view(item):
    if item is None:
        return False
    return (
        item.dedupe_key.startswith("notice:work-view:continuation:")
        or item.payload.get("work_view_intent", {}).get("origin", {}).get("kind") == "continuation"
        or item.payload.get("work_news_basis", {}).get("source", {}).get("work", {}).get("origin", {}).get("kind") == "continuation"
    )


def continuation_primary_id(item):
    prefix = "notice:work-view:continuation:"
    if not item.dedupe_key.startswith(prefix):
        return None
    fields = item.dedupe_key[len(prefix) :].split(":")
    if len(fields) != 2 or fields[1] not in {"refused", "expired"} or len(fields[0]) != 32 or any(char not in "0123456789abcdef" for char in fields[0]):
        return None
    return fields[0]


def continuation_origin(parent):
    state, source = continuation_state(parent), request_activation(parent)
    if not unadmitted_child(parent) or state["phase"] not in {"refused", "expired"} or source["outcome_id"] is None:
        raise ProductSourceRetired()
    return {"kind": "continuation", "act_id": parent.id, "anchor_outbox_id": source["outcome_id"], "signature": source_signature(parent, parent), "phase": state["phase"]}


class ContinuationViewOrigin:
    """Join actual primary consent, Native receipts and absent child namespace."""

    @classmethod
    async def prepare(cls, reader, link, item):
        origin = item.payload["work_view_intent"]["origin"]
        if type(origin) is not dict or set(origin) != {"kind", "act_id", "anchor_outbox_id", "signature", "phase"}:
            raise ProductSourceRetired()
        async with reader_session(reader.links.session_factory) as session:
            parent = await session.get(TelegramActRow, origin.get("act_id"))
            if parent is None:
                raise ProductSourceRetired()
            parent = _snapshot(parent)
        if continuation_origin(parent) != origin or continuation_primary_id(item) != parent.id or not item.dedupe_key.endswith(":" + origin["phase"]):
            raise ProductSourceRetired()
        return cls(reader, link, item, parent)

    def __init__(self, reader, link, item, parent):
        self.reader, self.link, self.item, self.parent = reader, link, item, parent
        self.origin = continuation_origin(parent)
        self.proof = RequestPrimaryProof(reader.acts, link, parent)

    def extend(self, query):
        parent = aliased(TelegramActRow)
        self.column = len(query.column_descriptions)
        result = query.add_columns(parent, child_namespace_owned(self.parent.id, continuation_state(self.parent)["child_id"])).outerjoin(parent, parent.id == self.parent.id)
        return self.proof.extend(result, parent)

    def check_with_permissions(self, rows, held):
        parent, occupied = rows[0][self.column : self.column + 2]
        try:
            if parent is None or any(row[self.column + 1] is not False for row in rows) or occupied is not False:
                raise ProductSourceRetired()
            actual = _snapshot(parent)
            if continuation_origin(actual) != self.origin or self.item.payload["work_view_intent"]["origin"] != self.origin:
                raise ProductSourceRetired()
            self.proof.check_primary(rows, actual)
            outcome = rows[0][self.proof.column + 3]
            if (
                (self.item.link_id, self.item.owner_user_id, self.item.binding_generation, self.item.chat_id, self.item.instance_id, self.item.work_id, self.item.message_thread_id)
                != (self.link.id, self.link.owner_user_id, self.link.binding_generation, self.link.chat_id, actual.instance_id, actual.work_id, outcome.message_thread_id)
                or self.item.payload["work_view_intent"]["work_id"] != actual.work_id
                or outcome.state != "sent"
            ):
                raise ProductSourceRetired()
            basis = outcome.payload["work_outcome_basis"]
            acknowledged_parts(outcome, basis["parts"], edit=basis.get("edit") is not None)
            if "work_outcome" in actual.continuation:
                expected = {"phase": phase(basis["context"]["act"]), "outbox_id": outcome.id}
                if not same_body(delivery_receipt(actual.continuation), expected):
                    raise ProductSourceRetired()
        except (KeyError, TypeError, ValueError, AttributeError):
            raise ProductSourceRetired() from None


async def offer_continuation_view(plans, link, act_id, *, guard):
    """Publication neither renews the old consent nor creates an activation."""

    async def perform():
        async with reader_session(plans.outbox.sessions) as reader:
            parent = await reader.get(TelegramActRow, act_id)
            if parent is None:
                return None
            try:
                parent = _snapshot(parent)
                origin = continuation_origin(parent)
            except (KeyError, TypeError, ValueError, ProductSourceRetired):
                return None
            primary = await reader.get(TelegramOutboxRow, origin["anchor_outbox_id"])
            if primary is None or primary.state != "sent":
                return None
            topic = primary.message_thread_id
        # Read current authority before creating a durable preview. Unknown
        # reads defer observation; the immutable primary remains independently saved.
        try:
            await plans.reader.detail(link, parent.work_id)
        except (AgentDenied, CeilingRefused, AccessOff):
            return None
        key = "notice:work-view:continuation:" + act_id + ":" + origin["phase"]
        candidate = plans._row(link, key, {"version": 1, "key": "view.detail", "work_id": parent.work_id, "origin": origin}, topic=topic)
        proof = ContinuationViewOrigin(plans.reader, link, _item(candidate), parent)
        admit = await prepare_attention(plans.reader, link)
        async with plans.callbacks.writer(link, guard) as writer:
            row = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == key).with_for_update())
            if row is None:
                await admit(session=writer, projection=proof)
                row = candidate
                writer.add(row)
                await writer.flush()
            identity = row.id
        await plans.materialize(link, identity, guard=guard)
        return identity

    return await await_drained(perform())
