"""One globally paced advisory offer; admission always needs a new human Start."""

import logging
from datetime import datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import and_, func, select

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.links import BotLink
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import PermanentDeliveryError, ProductSourceRetired, _item, keyboard_cleanup_row
from app.telegram_bots.work_capacity_sources import WorkCapacityOfferProjection, qualify_waiting, startable_detail
from app.telegram_bots.work_execution_sources import act_facts, execution_origin_rows, qualify_execution_origin
from app.telegram_bots.work_news_delivery import WorkNewsPlans, frozen_news
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)


async def retire_offer_controls(writer, old, *, outbox):
    """Malformed delivery custody cannot authorize touching another keyboard."""
    try:
        binding = await writer.get(TelegramBotLinkRow, old.link_id, populate_existing=True)
        if binding is None or (old.owner_user_id, old.binding_generation, old.chat_id, old.instance_id) != (binding.owner_user_id, binding.binding_generation, binding.chat_id, binding.agent_ref):
            raise ProductSourceRetired()
        item = _item(old)
        _basis, source, _part = frozen_news(SimpleNamespace(link=BotLink.from_row(binding), item=item, part=None), acknowledged=old.state == "sent")
        if source.key != "capacity.offer":
            raise ProductSourceRetired()
        parts = [part for part in item.parts if part["presentation_version"] == item.payload["presentation_version"]]
        groups = {part.get("callback_group_id") for part in parts} - {None}
        controls = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id.in_(groups)).with_for_update().execution_options(populate_existing=True)))
        _validate_rows(item, controls, allow_pending=True)
        for control in controls:
            part = next(part for part in parts if part.get("callback_group_id") == control.group_id)
            if control.message_id is not None and (part["state"] != "sent" or part["message_ids"] != [control.message_id]):
                raise ProductSourceRetired()
    except (ProductSourceRetired, PermanentDeliveryError, CallbackRefused, KeyError, ValueError, TypeError, IndexError):
        return
    cleaned = set()
    for control in controls:
        if control.consumed_at is None:
            control.consumed_at, control.closed_reason = outbox.clock(), "superseded"
        if control.message_id is not None and control.group_id not in cleaned:
            cleaned.add(control.group_id)
            writer.add(keyboard_cleanup_row(control, owner_user_id=old.owner_user_id, now=outbox.clock()))


async def waiting_origin(writer, link, journal, *, expected=None, origin=None, transfer=None):
    if link is None or journal is None:
        raise ProductSourceRetired()
    callback, parent, controls, primary = await execution_origin_rows(writer, journal)
    attempt = await writer.scalar(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.work_id == journal.work_id, AgentWorkAttemptRow.activation_id == journal.id).execution_options(populate_existing=True))
    origin = origin or qualify_execution_origin(link, _snapshot(journal), callback, parent, controls, activation_parent=primary)
    act = qualify_waiting(link, journal, attempt, callback, parent, controls, origin, expected=expected, transfer=transfer, activation_parent=primary)
    return act, origin, parent.message_thread_id


class WorkCapacityOffers(WorkNewsPlans):
    def __init__(self, *, acts, **kwargs):
        super().__init__(**kwargs, projection=WorkCapacityOfferProjection)
        self.acts = acts
        self.reader.acts = acts
        if acts.session_factory.kw.get("bind") is not self.outbox.sessions.kw.get("bind"):
            raise ValueError("work-capacity-requires-shared-storage")

    async def _offers(self, reader):
        return list(
            await reader.scalars(
                select(TelegramOutboxRow)
                .join(
                    TelegramBotLinkRow,
                    and_(
                        TelegramBotLinkRow.id == TelegramOutboxRow.link_id,
                        TelegramBotLinkRow.binding_generation == TelegramOutboxRow.binding_generation,
                        TelegramBotLinkRow.owner_user_id == TelegramOutboxRow.owner_user_id,
                        TelegramBotLinkRow.chat_id == TelegramOutboxRow.chat_id,
                        TelegramBotLinkRow.agent_ref == TelegramOutboxRow.instance_id,
                    ),
                )
                .where(TelegramOutboxRow.payload["capacity_offer_intent"]["act_id"].as_string().is_not(None), TelegramBotLinkRow.state == "connected", TelegramBotLinkRow.health == "ok")
                .order_by(TelegramOutboxRow.created_at, TelegramOutboxRow.id)
                .execution_options(populate_existing=True)
            )
        )

    def _held(self, rows):
        if not rows:
            return False
        latest = rows[-1]
        if latest.state == "skipped" and latest.skip_reason == "source-retired" and not latest.parts and all(getattr(latest, key) is None for key in ("claim_owner", "claim_id", "claimed_at")):
            # Verified absence of any presentation/SDK claim leaves no offer
            # with a human. Unknown or acknowledged publication still holds.
            return False
        if latest.state in {"pending", "sending"}:
            return True
        sent = [aware(p["sent_at"]) if not isinstance(p["sent_at"], str) else aware(datetime.fromisoformat(p["sent_at"])) for p in latest.parts if p.get("sent_at")]
        since = max(sent) if sent else aware(latest.created_at)
        return self.outbox.clock() < since + timedelta(seconds=self.outbox.config.outbox.place_free_hold_seconds)

    async def poll(self):
        return await await_drained(self._poll())

    async def _poll(self):
        # The database owns arbitration across bot processes. No process-local
        # lock or occupant query stands in for the authenticated HTTP budget.
        async with self.outbox.sessions() as reader:
            previous = await self._offers(reader)
            if previous and previous[-1].state == "pending" and not previous[-1].parts:
                # Resume a crash between saving an offer and its presentation.
                try:
                    link = await self.reader.links.get(previous[-1].link_id)
                    await self.materialize(link, previous[-1].id)
                except Exception as exc:
                    logger.debug("Capacity presentation unavailable (%s)", type(exc).__name__)
                    # No SDK claim can exist without parts. Retire only this
                    # advisory notice, never the original execution intent.
                    async with self.outbox._writer(previous[-1].link_id) as (writer, _binding):
                        saved = await writer.get(TelegramOutboxRow, previous[-1].id, with_for_update=True, populate_existing=True)
                        if saved is not None and saved.state == "pending" and not saved.parts and all(getattr(saved, key) is None for key in ("claim_owner", "claim_id", "claimed_at")):
                            saved.state, saved.skip_reason, saved.closed_at = "skipped", "source-retired", self.outbox.clock()
                previous = await self._offers(reader)
            if self._held(previous):
                return None
            rows = list(
                await reader.scalars(
                    select(TelegramActRow)
                    .join(
                        TelegramBotLinkRow,
                        and_(
                            TelegramBotLinkRow.id == TelegramActRow.link_id,
                            TelegramBotLinkRow.binding_generation == TelegramActRow.binding_generation,
                            TelegramBotLinkRow.owner_user_id == TelegramActRow.owner_user_id,
                            TelegramBotLinkRow.chat_id == TelegramActRow.chat_id,
                        ),
                    )
                    .where(TelegramActRow.kind == "activate", TelegramActRow.state == "waiting", TelegramActRow.reason == "work-limit", TelegramActRow.cancel_requested_at.is_(None))
                    .order_by(TelegramActRow.created_at, TelegramActRow.id)
                )
            )
            candidates = [_snapshot(row) for row in rows]
            offered = {row.payload["capacity_offer_intent"]["act_id"]: aware(row.created_at) for row in previous}
            counts = {}
            for row in previous:
                identity = row.payload["capacity_offer_intent"]["act_id"]
                counts[identity] = max(counts.get(identity, 0), row.payload["capacity_offer_intent"]["ordinal"])
        candidates.sort(key=lambda act: (act.id in offered, offered.get(act.id, aware(act.created_at)), aware(act.created_at), act.id))
        for captured in candidates:
            try:
                link = await self.reader.links.get(captured.link_id)
                async with self.outbox.sessions() as reader:
                    journal = await reader.get(TelegramActRow, captured.id)
                    act, origin, topic = await waiting_origin(reader, link, journal, expected=act_facts(captured))
                detail = await self.reader.detail(link, captured.work_id, ignore_activation=captured.id)
                current = await self.reader.prepare_detail(link, detail, ignore_activation=captured.id)
                admission = await prepare_attention(self.reader, link)
                eligible = startable_detail(detail, link)
                if eligible:
                    capacity = await self.reader.capacity.read(link)
                    if not capacity.capacity.available:
                        return None
                async with self.callbacks._writer() as writer:
                    if writer.get_bind().dialect.name == "postgresql":
                        await writer.execute(select(func.pg_advisory_xact_lock(427816395072451)))
                    latest = await self._offers(writer)
                    if self._held(latest):
                        return None
                    # Global -> bot -> journal/control is the fixed lock order.
                    link_ids = sorted({link.id, *(r.link_id for r in latest[-1:])})
                    await writer.scalars(select(TelegramBotLinkRow).where(TelegramBotLinkRow.id.in_(link_ids)).order_by(TelegramBotLinkRow.id).with_for_update().execution_options(populate_existing=True))
                    journal = await writer.get(TelegramActRow, captured.id, with_for_update=True, populate_existing=True)
                    await waiting_origin(writer, link, journal, expected=act_facts(act), origin=origin)
                    await current(session=writer)
                    if not eligible:
                        journal.state, journal.reason, journal.updated_at = "refused", "superseded", self.outbox.clock()
                        await writer.flush()
                        continue
                    await admission(session=writer)
                    for old in latest[-1:]:
                        await retire_offer_controls(writer, old, outbox=self.outbox)
                    ordinal = counts.get(act.id, 0) + 1
                    now = self.outbox.clock()
                    # A never-published notice can be replaced without a hold.
                    # Preserve total queue order even when the host clock has
                    # not advanced; random IDs must not select the active offer.
                    previous_time = await writer.scalar(select(func.max(TelegramOutboxRow.created_at)).where(TelegramOutboxRow.payload["capacity_offer_intent"]["act_id"].as_string().is_not(None)))
                    queued_at = max(now, aware(previous_time) + timedelta(microseconds=1)) if previous_time is not None else now
                    row = TelegramOutboxRow(
                        id=uuid4().hex,
                        link_id=link.id,
                        owner_user_id=link.owner_user_id,
                        binding_generation=link.binding_generation,
                        chat_id=link.chat_id,
                        instance_id=link.agent_ref,
                        work_id=act.work_id,
                        dedupe_key="free:" + act.id + ":" + str(ordinal),
                        class_="service",
                        state="pending",
                        parts=[],
                        silent=False,
                        payload={"presentation_version": 0, "capacity_offer_intent": {"version": 1, "act_id": act.id, "ordinal": ordinal, "origin": origin}},
                        message_thread_id=topic,
                        not_before=now,
                        created_at=queued_at,
                    )
                    writer.add(row)
                    await writer.flush()
                    identity = row.id
                await self.materialize(link, identity)
                return identity
            except Exception as exc:
                logger.debug("Capacity offer unavailable (%s)", type(exc).__name__)
                # Unknown source/provider state is never a refusal or a launch.
                # Continue independent owners; the durable original stays held.
                continue
        return None


async def transfer_offer(writer, link, original, operation_id, *, now):
    source = original.payload["work_news_basis"]["source"]
    if source["key"] != "capacity.offer":
        return
    expected = source["work"]["waiting_act"]
    journal = await writer.get(TelegramActRow, expected["id"], with_for_update=True, populate_existing=True)
    transfer = {"capacity_transfer": {"outbox_id": original.id, "operation_id": operation_id}}
    await waiting_origin(writer, link, journal, expected=expected, origin=source["work"]["origin"], transfer=transfer)
    journal.state, journal.reason, journal.result, journal.updated_at = "refused", "superseded", transfer, now
    await writer.flush()
