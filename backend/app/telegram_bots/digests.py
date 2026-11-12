"""Immutable digest reservations; rendering and source dispatch stay separate.

An empty digest is a durable intent, never an acknowledged message. Members
remain reserved through retry/restart until ACK or explicit pre-ACK abandonment.
"""

import hashlib
import json
from datetime import datetime
from uuid import uuid4

from sqlalchemy import case, select

from app.telegram_bots.config import DIGEST_MEMBERS_MAX
from app.telegram_bots.notifications import DISCRETIONARY_DELIVERY_CLASSES, ESSENTIAL_DELIVERY_CLASSES, aware, preference_skip
from app.telegram_bots.settings import _snapshot
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.utils.file_io import await_drained

MEMBER_CLASSES = (ESSENTIAL_DELIVERY_CLASSES - {"reply", "progress"}) | DISCRETIONARY_DELIVERY_CLASSES


def category_for(kind):
    if type(kind) is not str or kind not in MEMBER_CLASSES:
        raise ValueError("invalid-digest-member")
    return "discretionary" if kind in DISCRETIONARY_DELIVERY_CLASSES else "essential"


def key_for(category, identities):
    if type(category) is not str or category not in {"essential", "discretionary"}:
        raise ValueError("invalid-digest-category")
    digest = hashlib.sha256(json.dumps(identities, separators=(",", ":")).encode()).hexdigest()
    return f"digest:{category}:{digest}"


def validate(row):
    data = row.payload
    if type(data) is not dict:
        raise ValueError("invalid-digest-source")
    if "digest_dispatch_conflict" in data:
        raise ValueError("digest-reconciliation-required")
    identities, kinds, category = data.get("member_ids"), data.get("member_classes"), data.get("category")
    if (
        row.class_ != "digest"
        or type(data.get("digest_version")) is not int
        or data["digest_version"] != 1
        or type(identities) is not list
        or not 1 <= len(identities) <= DIGEST_MEMBERS_MAX
        or type(kinds) is not list
        or len(kinds) != len(identities)
    ):
        raise ValueError("invalid-digest-source")
    if any(type(identity) is not str or len(identity) != 32 or any(ch not in "0123456789abcdef" for ch in identity) for identity in identities) or len(set(identities)) != len(identities):
        raise ValueError("invalid-digest-members")
    if type(data.get("contains_questions")) is not bool or data["contains_questions"] != ("question" in kinds):
        raise ValueError("invalid-digest-priority")
    if any(category_for(kind) != category for kind in kinds) or row.dedupe_key != key_for(category, identities):
        raise ValueError("invalid-digest-source")
    stamps = data.get("member_created_at")
    if type(stamps) is not list or len(stamps) != len(identities):
        raise ValueError("invalid-digest-age")
    for stamp in stamps:
        if type(stamp) is not str:
            raise ValueError("invalid-digest-age")
        datetime.fromisoformat(stamp)
    # One digest card has one ACK. Expanded member previews are separate rows.
    if len(row.parts) > 1 or any(type(part) is not dict or part.get("kind") != "text" or part.get("state") not in {"pending", "sending", "sent"} for part in row.parts):
        raise ValueError("invalid-digest-parts")
    return identities, kinds, category


def unsolicited(row):
    return (
        row.class_ in MEMBER_CLASSES
        and row.dedupe_key.startswith(("event:", "ev:", "summary:", "remind:", "rejected:", "free:"))
        and not row.payload.get("digest_id")
        and bool(row.parts)
        and all(part.get("kind") != "edit" and part.get("state") == "pending" and not part.get("dispatched_at") and not part.get("sent_at") for part in row.parts)
    )


class DigestReservations:
    def __init__(self, outbox):
        self.outbox = outbox

    async def reserve(self, link, *, category):
        if type(category) is not str or category not in {"essential", "discretionary"}:
            raise ValueError("invalid-digest-category")
        return await await_drained(self._reserve(link, category))

    async def _reserve(self, link, category):
        from app.telegram_bots.producer import _item

        async with self.outbox._writer(link.id) as (writer, binding):
            if not self.outbox._binding(binding, link) or binding.state != "connected" or binding.health != "ok":
                return None
            settings = await writer.get(TelegramPersonSettingsRow, link.owner_user_id)
            if settings is None:
                return None
            choices = _snapshot(settings)
            now = self.outbox.clock()
            kinds = {kind for kind in MEMBER_CLASSES if category_for(kind) == category}
            query = (
                select(TelegramOutboxRow)
                .where(
                    TelegramOutboxRow.link_id == link.id,
                    TelegramOutboxRow.owner_user_id == link.owner_user_id,
                    TelegramOutboxRow.binding_generation == link.binding_generation,
                    TelegramOutboxRow.chat_id == link.chat_id,
                    TelegramOutboxRow.state == "pending",
                    TelegramOutboxRow.class_.in_(kinds),
                    TelegramOutboxRow.not_before <= now,
                    TelegramOutboxRow.payload["digest_id"].as_string().is_(None),
                )
                .order_by(case((TelegramOutboxRow.class_ == "question", 0), else_=1), TelegramOutboxRow.created_at, TelegramOutboxRow.id)
            )
            members, offset = [], 0
            while len(members) < DIGEST_MEMBERS_MAX:
                page = list(await writer.scalars(query.limit(100).offset(offset).with_for_update()))
                for row in page:
                    if unsolicited(row) and preference_skip(_item(row), choices, now) is None:
                        members.append(row)
                        if len(members) == DIGEST_MEMBERS_MAX:
                            break
                if len(page) < 100:
                    break
                offset += len(page)
            if len(members) < self.outbox.config.outbox.digest_threshold:
                return None
            identities = [member.id for member in members]
            key = key_for(category, identities)
            # A terminated batch is not silently resurrected. Its now-free
            # members may deliver individually or join a different batch.
            if await writer.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == key)):
                return None
            identity = uuid4().hex
            digest = TelegramOutboxRow(
                id=identity,
                link_id=link.id,
                owner_user_id=link.owner_user_id,
                binding_generation=link.binding_generation,
                chat_id=link.chat_id,
                instance_id=link.agent_ref if link.agent_kind == "instance" else None,
                dedupe_key=key,
                class_="digest",
                payload={
                    "digest_version": 1,
                    "category": category,
                    "member_ids": identities,
                    "member_classes": [member.class_ for member in members],
                    "member_created_at": [aware(member.created_at).isoformat() for member in members],
                    "contains_questions": any(member.class_ == "question" for member in members),
                    "presentation_version": 0,
                },
                parts=[],
                state="pending",
                silent=all(bool(member.silent) or all(part.get("silent") is True for part in member.parts) or member.class_ in {"summary", "reminder"} for member in members),
                not_before=now,
                created_at=now,
            )
            writer.add(digest)
            for member in members:
                member.payload = {**member.payload, "digest_id": identity}
            await writer.flush()
            return identity

    async def abandon(self, link, identity, *, reason="superseded"):
        from app.telegram_bots.producer import _SKIP_REASONS

        if reason not in _SKIP_REASONS or reason == "in-digest":
            raise ValueError("invalid-digest-abandon-reason")

        async def perform():
            async with self.outbox._writer(link.id) as (writer, binding):
                if not self.outbox._binding(binding, link):
                    raise ValueError("digest-binding-changed")
                row = await writer.get(TelegramOutboxRow, identity, with_for_update=True)
                if row is None or row.link_id != link.id or row.binding_generation != link.binding_generation or row.class_ != "digest":
                    raise ValueError("invalid-digest-source")
                if row.state == "skipped":
                    return
                if row.state == "sending":
                    raise ValueError("digest-claim-active")
                await abandon_members(writer, row, now=self.outbox.clock())
                row.state, row.skip_reason, row.closed_at = "skipped", reason, self.outbox.clock()
                row.claim_owner = row.claim_id = row.claimed_at = None

        return await await_drained(perform())


async def _members(writer, parent):
    identities, kinds, _category = validate(parent)
    rows = {row.id: row for row in await writer.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.id.in_(identities)).order_by(TelegramOutboxRow.id).with_for_update())}
    if set(rows) != set(identities):
        raise ValueError("digest-member-missing")
    ordered = [rows[identity] for identity in identities]
    for row, kind, stamp in zip(ordered, kinds, parent.payload["member_created_at"], strict=True):
        if aware(row.created_at) != aware(datetime.fromisoformat(stamp)):
            raise ValueError("digest-member-age-changed")
        if (row.link_id, row.owner_user_id, row.binding_generation, row.chat_id, row.class_, row.payload.get("digest_id")) != (parent.link_id, parent.owner_user_id, parent.binding_generation, parent.chat_id, kind, parent.id):
            raise ValueError("digest-member-changed")
    return ordered


async def claim_members(writer, parent):
    members = await _members(writer, parent)
    for row in members:
        if row.state != "pending" or row.claim_id is not None:
            raise ValueError("digest-member-already-claimed")
    for row in members:
        row.state, row.claim_owner, row.claim_id, row.claimed_at = "sending", parent.claim_owner, parent.claim_id, parent.claimed_at


async def release_members(writer, parent, *, owner, claim):
    if parent.class_ != "digest":
        return
    rows = await writer.scalars(
        select(TelegramOutboxRow)
        .where(TelegramOutboxRow.link_id == parent.link_id, TelegramOutboxRow.payload["digest_id"].as_string() == parent.id, TelegramOutboxRow.state == "sending", TelegramOutboxRow.claim_owner == owner, TelegramOutboxRow.claim_id == claim)
        .with_for_update()
    )
    for row in rows:
        row.state = "pending"
        row.claim_owner = row.claim_id = row.claimed_at = None


async def acknowledge_members(writer, parent, *, now):
    members = await _members(writer, parent)
    if len(parent.parts) != 1:
        raise ValueError("digest-ack-required")
    part = parent.parts[0]
    identities, stamp = part.get("message_ids"), part.get("sent_at")
    if (
        part.get("state") != "sent"
        or part.get("presentation_version") != parent.payload.get("presentation_version")
        or type(identities) is not list
        or len(identities) != 1
        or type(identities[0]) is not int
        or identities[0] <= 0
        or type(stamp) is not str
    ):
        raise ValueError("digest-ack-required")
    if datetime.fromisoformat(stamp).tzinfo is None:
        raise ValueError("digest-ack-required")
    for row in members:
        if (row.state, row.claim_owner, row.claim_id) != ("sending", parent.claim_owner, parent.claim_id):
            raise ValueError("digest-member-claim-changed")
    groups = set()
    for row in members:
        row.state, row.skip_reason, row.closed_at = "skipped", "in-digest", now
        row.claim_owner = row.claim_id = row.claimed_at = None
        groups.update(part.get("callback_group_id") for part in row.parts)
    await _retire_controls(writer, parent, groups - {None}, now)


async def _retire_controls(writer, parent, groups, now):
    for row in await writer.scalars(
        select(TelegramCallbackRow).where(TelegramCallbackRow.link_id == parent.link_id, TelegramCallbackRow.binding_generation == parent.binding_generation, TelegramCallbackRow.group_id.in_(groups)).with_for_update()
    ):
        if row.consumed_at is None:
            row.consumed_at, row.closed_reason = now, "superseded"


async def abandon_members(writer, parent, *, now):
    if "digest_dispatch_conflict" in parent.payload:
        raise ValueError("digest-reconciliation-required")
    if parent.state == "sent" or any(type(part) is dict and (part.get("state") == "sent" or part.get("sent_at")) for part in parent.parts):
        raise ValueError("digest-already-acknowledged")
    await _retire_controls(writer, parent, {part.get("callback_group_id") for part in parent.parts if type(part) is dict} - {None}, now)
    rows = await writer.scalars(
        select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == parent.link_id, TelegramOutboxRow.binding_generation == parent.binding_generation, TelegramOutboxRow.payload["digest_id"].as_string() == parent.id).with_for_update()
    )
    for row in rows:
        if row.state == "sending" and (row.claim_owner, row.claim_id) != (parent.claim_owner, parent.claim_id):
            continue  # Another owner is never released by this retirement.
        if row.state not in {"pending", "sending"}:
            continue
        row.payload = {key: value for key, value in row.payload.items() if key != "digest_id"}
        row.state = "pending"
        row.claim_owner = row.claim_id = row.claimed_at = None
