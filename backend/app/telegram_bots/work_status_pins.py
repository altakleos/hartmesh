"""Dormant, durable pin ownership for acknowledged Work status messages."""

from dataclasses import asdict
from datetime import datetime
from types import SimpleNamespace

from sqlalchemy import String, cast, or_, select
from sqlalchemy.orm import aliased
from telegram.error import BadRequest, Forbidden, InvalidToken, NetworkError, RetryAfter

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.links import BotLink
from app.telegram_bots.producer import DeliveryClaimRetired, DeliveryRetry, PermanentDeliveryError, ProductSourceRetired, _item
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_news_delivery import frozen_news
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_status import StatusProjection
from app.telegram_bots.work_status_updates import anchor, validate_update
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def steps(root):
    values = root.payload.get("status_pin_steps", [])
    if type(values) is not list or any(type(value) is not str or len(value) != 32 for value in values) or len(values) != len(set(values)) or root.payload.get("status_pin_tail") != (values[-1] if values else None):
        raise ProductSourceRetired()
    return values


def recipe(operation, message):
    return {"kind": "pin", "source_ref": {"key": "work.status.pin", "values": {}}, "operation": operation, "target_message_id": message, "silent": True, "protect_content": False}


def root_identity(payload):
    basis = payload.get("work_status_pin")
    identity = basis.get("root_id") if type(basis) is dict else None
    if type(identity) is not str or len(identity) != 32 or any(char not in "0123456789abcdef" for char in identity):
        raise ProductSourceRetired()
    return identity


def presentation_proof(link, root, presentation):
    if (
        presentation is None
        or presentation.state != "sent"
        or (presentation.link_id, presentation.owner_user_id, presentation.binding_generation, presentation.chat_id, presentation.instance_id, presentation.work_id)
        != (root.link_id, root.owner_user_id, root.binding_generation, root.chat_id, root.instance_id, root.work_id)
    ):
        raise ProductSourceRetired()
    original = _item(root)
    root_basis, _source, _part = frozen_news(SimpleNamespace(link=link, item=original, part=original.parts[-1]), acknowledged=True)
    if presentation.id == root.id:
        basis = root_basis
        if basis["concealed"]:
            raise ProductSourceRetired()
    else:
        basis = validate_update(presentation, acknowledged=True)
        if basis["anchor"]["outbox_id"] != root.id or basis["anchor"]["source_signature"] != digest(root_basis):
            raise ProductSourceRetired()
    version = presentation.payload["presentation_version"]
    parts = [part for part in presentation.parts if part["presentation_version"] == version]
    return {"presentation_id": presentation.id, "presentation_signature": digest({"basis": basis, "parts": presentation.parts, "version": version})}, parts[0]["message_ids"][0]


def validate(row, root, ordinal):
    try:
        basis = row.payload["work_status_pin"]
        if (
            type(basis) is not dict
            or set(basis) != {"version", "root_id", "attempt_id", "origin_signature", "operation", "message_id", "sequence", "presentation_id", "presentation_signature"}
            or type(basis["version"]) is not int
            or basis["version"] != 1
            or type(basis["sequence"]) is not int
            or basis["sequence"] != ordinal
            or basis["root_id"] != root.id
            or basis["attempt_id"] != root.payload["work_event_intent"]["attempt_id"]
            or basis["operation"] not in {"pin", "unpin"}
            or type(basis["message_id"]) is not int
            or basis["message_id"] <= 0
            or (row.link_id, row.owner_user_id, row.binding_generation, row.chat_id, row.instance_id, row.work_id, row.class_, row.silent)
            != (root.link_id, root.owner_user_id, root.binding_generation, root.chat_id, root.instance_id, root.work_id, "progress", True)
            or row.dedupe_key != f"status-pin:{root.id}:{ordinal}"
            or type(row.payload.get("presentation_version")) is not int
            or row.payload["presentation_version"] != 0
            or len(row.parts) != 1
        ):
            raise ProductSourceRetired()
        part = row.parts[0]
        expected = recipe(basis["operation"], basis["message_id"])
        if any(not same_body(part.get(key), value) for key, value in expected.items()) or set(part) - set(expected) - {"part_id", "presentation_version", "ordinal", "state", "message_ids", "callback_group_id", "sent_at", "dispatched_at"}:
            raise ProductSourceRetired()
        if part.get("callback_group_id") is not None or type(part.get("ordinal")) is not int or part["ordinal"] != 0 or type(part.get("presentation_version")) is not int or part["presentation_version"] != 0:
            raise ProductSourceRetired()
        identity = part.get("part_id")
        if type(identity) is not str or len(identity) != 32 or any(value not in "0123456789abcdef" for value in identity) or part.get("state") not in {"pending", "sending", "sent"}:
            raise ProductSourceRetired()
        for stamp in ("sent_at", "dispatched_at"):
            if stamp in part and (type(part[stamp]) is not str or datetime.fromisoformat(part[stamp]).utcoffset() is None):
                raise ProductSourceRetired()
        if part["state"] == "sent":
            if type(part.get("message_ids")) is not list or len(part["message_ids"]) != 1 or type(part["message_ids"][0]) is not int or part["message_ids"][0] != basis["message_id"] or "sent_at" not in part:
                raise ProductSourceRetired()
        elif part.get("message_ids") != [] or "sent_at" in part:
            raise ProductSourceRetired()
        if row.state == "sent" and part["state"] != "sent":
            raise ProductSourceRetired()
        return basis
    except (AttributeError, KeyError, TypeError, ValueError, IndexError):
        raise ProductSourceRetired() from None


def ledger(link, root, operations, presentations, origin):
    identities = steps(root)
    pinned, unfinished, failed, confirmed, pin_basis = None, None, None, False, None
    for ordinal, identity in enumerate(identities):
        operation = operations.get(identity)
        basis = validate(operation, root, ordinal)
        proof, message = presentation_proof(link, root, presentations.get(identity))
        if basis["origin_signature"] != origin or message != basis["message_id"] or any(basis[key] != value for key, value in proof.items()):
            raise ProductSourceRetired()
        if operation.state in {"pending", "sending"}:
            if ordinal != len(identities) - 1:
                raise ProductSourceRetired()
            unfinished = operation
        elif operation.state == "failed":
            if any(getattr(operation, key) is not None for key in ("claim_id", "claim_owner", "claimed_at")):
                raise DeliveryClaimRetired()
            failed = operation
        if basis["operation"] == "pin" and (operation.state == "sent" or operation.parts[0].get("dispatched_at")):
            confirmed = operation.state == "sent" or pinned == basis["message_id"] and confirmed
            pinned, pin_basis = basis["message_id"], proof
        elif basis["operation"] == "unpin" and operation.state == "sent":
            if pinned != basis["message_id"]:
                raise ProductSourceRetired()
            pinned, confirmed, pin_basis = None, False, None
        elif basis["operation"] == "unpin" and operation.parts[0].get("dispatched_at"):
            confirmed = False
    return pinned, unfinished, failed, confirmed, pin_basis


async def pin_cleanup_anchors(session):
    """Malformed or unfinished pin history is retained, never trusted as cleanup."""
    roots = list(
        await session.execute(
            select(TelegramOutboxRow, TelegramBotLinkRow)
            .join(TelegramBotLinkRow, TelegramBotLinkRow.id == TelegramOutboxRow.link_id)
            .where(or_(TelegramOutboxRow.payload["status_pin_steps"][0].as_string().is_not(None), TelegramOutboxRow.payload["status_pin_tail"].as_string().is_not(None)))
        )
    )
    pinned, identities = set(), set()
    for root, _link in roots:
        try:
            identities.update(steps(root))
        except ProductSourceRetired:
            pinned.add(root.id)
    operations, presentations = {}, {}
    presentation = aliased(TelegramOutboxRow)
    ordered = sorted(identities)
    for offset in range(0, len(ordered), 500):
        values = await session.execute(
            select(TelegramOutboxRow, presentation).outerjoin(presentation, presentation.id == TelegramOutboxRow.payload["work_status_pin"]["presentation_id"].as_string()).where(TelegramOutboxRow.id.in_(ordered[offset : offset + 500]))
        )
        for operation, source in values:
            operations[operation.id], presentations[operation.id] = operation, source
    for root, link in roots:
        try:
            origin = root.payload["work_news_basis"]["source"]["event_signature"]
            target, unfinished, _failed, _confirmed, _proof = ledger(BotLink.from_row(link), root, operations, presentations, origin)
            if target is not None or unfinished is not None:
                pinned.add(root.id)
        except (ProductSourceRetired, PermanentDeliveryError, DeliveryClaimRetired, KeyError, IndexError, TypeError, ValueError, AttributeError):
            pinned.add(root.id)
    return pinned


class _Projection(StatusProjection):
    def __init__(self, host, link, root, clock, *, captured=None):
        super().__init__(link, _item(root), clock)
        self.host, self.root, self.identities, self.captured = host, root, steps(root), captured

    def extend(self, query):
        query = super().extend(query)
        self.extra = len(query.column_descriptions)
        moved, operation, presentation = aliased(TelegramOutboxRow), aliased(TelegramOutboxRow), aliased(TelegramOutboxRow)
        pointer = self.root.payload.get("status_anchor", {})
        if type(pointer) is not dict:
            raise ProductSourceRetired()
        return (
            query.add_columns(moved, operation, presentation)
            .outerjoin(moved, moved.id == pointer.get("outbox_id"))
            .outerjoin(operation, operation.id.in_(self.identities))
            .outerjoin(presentation, presentation.id == operation.payload["work_status_pin"]["presentation_id"].as_string())
        )

    def check(self, rows):
        super().check(rows)
        root, moved = rows[0][self.column + 4], rows[0][self.extra]
        context = {"status": asdict(self.source), "revision": rows[0][self.column].revision, "work_id": root.work_id, "locale": self.clock.settings.locale}
        self.target = anchor(self.link, root, context, moved)
        self.presentation = moved if moved is not None else root
        operations = {row[self.extra + 1].id: row[self.extra + 1] for row in rows if row[self.extra + 1] is not None}
        presentations = {row[self.extra + 1].id: row[self.extra + 2] for row in rows if row[self.extra + 1] is not None}
        if set(operations) != set(self.identities):
            raise ProductSourceRetired()
        self.pinned, self.unfinished, self.failed, self.confirmed, self.pin_basis = ledger(self.link, root, operations, presentations, self.source.origin_signature)
        self.desired = None if self.source.terminal else self.target["messages"][0]["id"]
        if self.captured is not None:
            saved = self.captured
            if not self.identities or self.identities[-1] != saved.item.id:
                raise DeliveryClaimRetired()
            operation = operations[saved.item.id]
            current = self.host.outbox._sending(rows[0][0], operation, link_id=self.link.id, part_id=saved.part["part_id"], claim_owner=saved.claim_owner, claim_id=saved.claim_id, token_revision=saved.token_revision)
            if current != saved:
                raise DeliveryClaimRetired()
            basis = validate(operation, root, len(self.identities) - 1)
            if basis["operation"] == "pin" and basis["message_id"] != self.desired:
                raise ProductSourceRetired()
            if basis["operation"] == "unpin" and (basis["message_id"] != self.pinned or self.desired == self.pinned):
                raise ProductSourceRetired()


class WorkStatusPins:
    def __init__(self, updates):
        self.updates, self.reader, self.outbox = updates, updates.reader, updates.outbox

    async def delete_service(self, channel, link, message):
        """Ingress owns this exact bot-authored service event through deletion."""
        from app.telegram_bots.adapter import LiveRequestFence
        from app.telegram_bots.authority import AccessOff, CeilingRefused
        from deerflow.agent_instances.contract import AgentDenied

        pinned = message.pinned_message
        if (
            pinned is None
            or message.from_user is None
            or not message.from_user.is_bot
            or message.from_user.id != link.bot_id
            or message.chat_id != link.chat_id
            or pinned.chat.id != link.chat_id
            or type(message.message_id) is not int
            or message.message_id <= 0
            or message.message_id == pinned.message_id
        ):
            return False

        async def perform():
            async with self.outbox.sessions() as reader:
                operation = await reader.scalar(
                    select(TelegramOutboxRow)
                    .where(
                        TelegramOutboxRow.link_id == link.id,
                        TelegramOutboxRow.binding_generation == link.binding_generation,
                        TelegramOutboxRow.payload["work_status_pin"]["operation"].as_string() == "pin",
                        cast(TelegramOutboxRow.payload["work_status_pin"]["message_id"].as_string(), String) == str(pinned.message_id),
                    )
                    .order_by(TelegramOutboxRow.created_at.desc())
                    .limit(1)
                )
                if operation is None:
                    return False
                identity, root_id = operation.id, root_identity(operation.payload)

            async def current():
                if not channel._callback_current(link):
                    raise DeliveryClaimRetired()
                async with self.outbox.sessions() as reader:
                    root = await reader.get(TelegramOutboxRow, root_id)
                    if root is None:
                        raise ProductSourceRetired()
                clock = await self.reader.prepare(link)
                admission = await prepare_attention(self.reader, link)
                await admission(projection=_ServiceProjection(self, link, root, clock, identity=identity, message=pinned.message_id))
                if not channel._callback_current(link):
                    raise DeliveryClaimRetired()

            try:
                await current()
                result = await channel.bot.delete_message(chat_id=link.chat_id, message_id=message.message_id, rate_limit_args=LiveRequestFence(current))
            except (AccessOff, CeilingRefused, AgentDenied, ProductSourceRetired):
                return False
            except BadRequest as error:
                if "message to delete not found" not in str(error).casefold():
                    raise
                result = True
            if result is not True:
                raise NetworkError("Service deletion acknowledgement unavailable")
            return True

        return await await_drained(perform())

    async def plan(self, link, root_id):
        async def perform():
            clock = await self.reader.prepare(link)
            admission = await prepare_attention(self.reader, link)
            async with self.outbox._writer(link.id) as (writer, binding):
                if not self.outbox._binding(binding, link):
                    raise ProductSourceRetired()
                root = await writer.get(TelegramOutboxRow, root_id, with_for_update=True, populate_existing=True)
                if root is None:
                    raise ProductSourceRetired()
                projection = _Projection(self, link, root, clock)
                await admission(session=writer, projection=projection)
                if projection.unfinished is not None:
                    return projection.unfinished.id
                if projection.failed is not None and projection.failed.id == steps(root)[-1]:
                    failed = projection.failed.payload["work_status_pin"]
                    # A refused physical target waits for a changed presentation.
                    # A possibly dispatched old pin must still be unpinned first.
                    if failed["operation"] == "unpin" or failed["message_id"] == projection.desired:
                        return None
                if projection.pinned == projection.desired and (projection.desired is None or projection.confirmed):
                    return None
                operation = "unpin" if projection.pinned is not None and projection.pinned != projection.desired else "pin"
                message = projection.pinned if operation == "unpin" else projection.desired
                proof = projection.pin_basis if operation == "unpin" else presentation_proof(link, root, projection.presentation)[0]
                ordinal = len(steps(root))
                basis = {"version": 1, "root_id": root.id, "attempt_id": projection.source.attempt_id, "origin_signature": projection.source.origin_signature, "operation": operation, "message_id": message, "sequence": ordinal, **proof}
                identity, _ = await self.outbox._add(
                    link, dedupe_key=f"status-pin:{root.id}:{ordinal}", class_="progress", payload={"work_status_pin": basis, "parts": [recipe(operation, message)]}, instance_id=root.instance_id, work_id=root.work_id, session=writer
                )
                root.payload = {**root.payload, "status_pin_steps": [*steps(root), identity], "status_pin_tail": identity}
                await writer.flush()
                await admission(session=writer, projection=_Projection(self, link, root, clock))
                return identity

        return await await_drained(perform())

    async def repair(self, link, identity):
        """Retry only a claim-free failed cleanup of the same possibly pinned ID."""

        async def perform():
            clock = await self.reader.prepare(link)
            admission = await prepare_attention(self.reader, link)
            async with self.outbox._writer(link.id) as (writer, binding):
                if not self.outbox._binding(binding, link):
                    raise ProductSourceRetired()
                row = await writer.get(TelegramOutboxRow, identity, with_for_update=True, populate_existing=True)
                if row is None or row.state != "failed" or any(getattr(row, key) is not None for key in ("claim_id", "claim_owner", "claimed_at")):
                    return False
                root = await writer.get(TelegramOutboxRow, root_identity(row.payload), with_for_update=True, populate_existing=True)
                if root is None or not steps(root) or steps(root)[-1] != identity:
                    raise ProductSourceRetired()
                projection = _Projection(self, link, root, clock)
                await admission(session=writer, projection=projection)
                basis = validate(row, root, len(steps(root)) - 1)
                if basis["operation"] != "unpin" or row.parts[0]["state"] == "sent" or projection.pinned != basis["message_id"] or projection.desired == projection.pinned:
                    return False
                row.parts = [{**row.parts[0], "state": "pending"}]
                row.state, row.not_before = "pending", self.outbox.clock()
                row.failed_reason = row.closed_at = None
                await writer.flush()
                await admission(session=writer, projection=_Projection(self, link, root, clock))
                return True

        return await await_drained(perform())

    async def send(self, channel, saved):
        from app.telegram_bots.adapter import LiveRequestFence, LongRetryAfter

        async def current():
            if not channel._callback_current(saved.link):
                raise DeliveryClaimRetired()
            async with self.outbox.sessions() as reader:
                root = await reader.get(TelegramOutboxRow, root_identity(saved.item.payload))
                if root is None:
                    raise ProductSourceRetired()
            clock = await self.reader.prepare(saved.link)
            admission = await prepare_attention(self.reader, saved.link)
            await admission(projection=_Projection(self, saved.link, root, clock, captured=saved))
            if not channel._callback_current(saved.link):
                raise DeliveryClaimRetired()

        async def perform():
            await current()
            basis = saved.item.payload["work_status_pin"]
            method = "pin_chat_message" if basis["operation"] == "pin" else "unpin_chat_message"
            options = {"disable_notification": True} if basis["operation"] == "pin" else {}
            try:
                result = await getattr(channel.bot, method)(chat_id=saved.link.chat_id, message_id=basis["message_id"], rate_limit_args=LiveRequestFence(current), **options)
            except (LongRetryAfter, RetryAfter) as error:
                raise DeliveryRetry(error.seconds if isinstance(error, LongRetryAfter) else error._retry_after.total_seconds()) from None
            except InvalidToken as error:
                await channel.verify_token(error)
                raise DeliveryRetry(5) from None
            except Forbidden:
                await channel._forbidden(channel._context(saved.link, saved.link.chat_id, "bound_message"))
                raise
            except BadRequest as error:
                if basis["operation"] != "unpin" or "message to unpin not found" not in str(error).casefold():
                    raise PermanentDeliveryError() from None
                result = True
            if result is not True:
                raise NetworkError("Pin acknowledgement unavailable")
            await channel._after_any_success()
            return [basis["message_id"]]

        return await await_drained(perform())


class _ServiceProjection(_Projection):
    def __init__(self, *args, identity, message, **kwargs):
        super().__init__(*args, **kwargs)
        self.identity, self.message = identity, message

    def check(self, rows):
        super().check(rows)
        row = next((values[self.extra + 1] for values in rows if values[self.extra + 1] is not None and values[self.extra + 1].id == self.identity), None)
        if row is None or row.payload["work_status_pin"]["operation"] != "pin" or row.payload["work_status_pin"]["message_id"] != self.message or row.state != "sent" and not row.parts[0].get("dispatched_at"):
            raise ProductSourceRetired()
