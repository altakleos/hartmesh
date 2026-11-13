"""Retry saved delivery through its source owner before clearing failure state."""

import logging

from sqlalchemy import select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_scope import attention_proof
from app.telegram_bots.callbacks import CallbackRefused, _row, _validate_rows, canonical_keyboard
from app.telegram_bots.links import BotLink, prepare_publication_guard
from app.telegram_bots.producer import OutboxSender, _item
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)
_REASONS = {
    "summary-refresh-required",
    "summary-view-refresh-required",
    "digest-view-refresh-required",
    "work-status-refresh-required",
    "work-question-refresh-required",
    "work-news-refresh-required",
    "work-result-refresh-required",
    "work-routing-refresh-required",
    "work-outcome-refresh-required",
}


class NotificationRetries:
    def __init__(self, runtime):
        self.runtime, self.host, self.after = runtime, runtime.host, ""

    def repair(self, row):
        host, payload, reason = self.host, row.payload, row.failed_reason
        owners = (
            ("summary_basis", "summary-refresh-required", "summary_sources"),
            ("summary_view_basis", "summary-view-refresh-required", "summary_view_sources"),
            ("work_digest_view_basis", "digest-view-refresh-required", "work_digest_view_sources"),
            ("result_notice_basis", "work-result-refresh-required", "result_notice_sources"),
        )
        for family, expected, attribute in owners:
            if family in payload and (reason == expected or row.state == "skipped" and row.skip_reason == "superseded"):
                return getattr(host, attribute).recovery.repair
        if reason == "work-status-refresh-required" and "work_status_update" in payload:
            return host.work_status_update_sources.repair
        if reason == "work-question-refresh-required" and "work_question_update" in payload:
            return host.work_question_edit_sources.repair
        if reason == "work-news-refresh-required" and "work_news_basis" in payload:
            return (host.work_status_sources if row.dedupe_key.startswith("progress:") else host.work_news_sources).recovery.repair
        if "work_request_basis" in payload:
            if reason == "work-result-refresh-required":
                return host.work_callbacks.result_recovery.repair
            if reason == "work-routing-refresh-required":
                return host.work_callbacks.routing_recovery.repair
        if reason == "work-outcome-refresh-required" and "work_outcome_basis" in payload:
            act_id = payload["work_outcome_basis"]["context"]["act"]["id"]

            async def outcome(link, _identity, *, guard):
                return await self.runtime.outcomes.plan(link, act_id, guard=guard)

            return outcome
        return None

    async def execute(self, link, callback, *, guard):
        async def perform():
            if not self.runtime.available():
                raise CallbackRefused()
            selected_guard = _RetryGuard(self.host, link, callback, guard)
            await selected_guard.prepare()
            async with self.host.callbacks.writer(link, selected_guard) as writer:
                target = await writer.get(TelegramOutboxRow, callback.target_id)
                if target is None or (target.link_id, target.owner_user_id, target.binding_generation, target.chat_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id):
                    raise CallbackRefused()
                if any(getattr(target, name) is not None for name in ("claim_owner", "claim_id", "claimed_at")) or any(key.endswith("dispatch_conflict") for key in target.payload):
                    return True
                repair = self.repair(target)
                held = target.failed_reason in _REASONS
            if repair is None:
                return held
            with attention_proof(selected_guard.projection):
                await repair(link, callback.target_id, guard=selected_guard)
            return True

        return await await_drained(perform())

    async def prepare_notices(self):
        if not self.runtime.available() or self.runtime.events is None:
            return
        async with self.host.outbox.sessions() as reader:
            rows = (
                await reader.execute(
                    select(TelegramOutboxRow.id, TelegramBotLinkRow)
                    .join(TelegramBotLinkRow, TelegramBotLinkRow.id == TelegramOutboxRow.link_id)
                    .where(TelegramOutboxRow.id > self.after, TelegramOutboxRow.state == "failed", TelegramOutboxRow.failed_reason.in_(_REASONS), TelegramBotLinkRow.state == "connected", TelegramBotLinkRow.health == "ok")
                    .order_by(TelegramOutboxRow.id)
                    .limit(self.host.config.producer.batch)
                )
            ).all()
        for identity, binding in rows:
            if not self.runtime.available():
                break
            self.after = identity
            link = BotLink.from_row(binding)
            channel = self.host.channel(link.id)
            if channel is None:
                continue
            guard = self.host.commands._guard(channel, link)
            try:
                async with self.host.callbacks.writer(link, guard) as writer:
                    row = await writer.get(TelegramOutboxRow, identity, with_for_update=True, populate_existing=True)
                    if (
                        not self.runtime.available()
                        or row is None
                        or row.state != "failed"
                        or row.failed_reason not in _REASONS
                        or any(getattr(row, name) is not None for name in ("claim_owner", "claim_id", "claimed_at"))
                        or any(key.endswith("dispatch_conflict") for key in row.payload)
                        or self.repair(row) is None
                    ):
                        continue
                    version = row.payload["presentation_version"]
                    if row.payload.get("source_failure_notice_version") == version:
                        continue
                    failure_version = row.payload.get("failure_version", 0) + 1
                    row.payload = {**row.payload, "failure_version": failure_version, "source_failure_notice_version": version}
                    key = f"delivery-failed:{row.id}:{failure_version}"
                    if await writer.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == key)):
                        continue
                    notice = OutboxSender._notice_row(row, "delivery.failed", kind="delivery_failure_notice", dedupe_key=key, now=self.host.outbox.clock(), repair=True)
                    writer.add(notice)
                    await writer.flush()
                    group = await self.host.callbacks.write_repair_group(link, failed_outbox_id=row.id, anchor_outbox_id=notice.id, session=writer)
                    notice.parts = [{**part, "callback_group_id": group.group_id} for part in notice.parts]
                    await writer.flush()
                    await guard.check(writer)
                    if not self.runtime.available():
                        raise CallbackRefused()
            except Exception:
                logger.warning("Telegram delivery repair notice deferred: delivery=%s", identity)
        if len(rows) < self.host.config.producer.batch:
            self.after = ""


class _RetryGuard:
    def __init__(self, host, link, callback, base):
        self.host, self.link, self.callback, self.base = host, link, callback, base
        self.group = self.parent = None

    @property
    def captured_link(self):
        return self.base.captured_link

    def current(self):
        self.base.current()

    async def prepare(self):
        await prepare_publication_guard(self.base)

    async def check(self, writer):
        group = await self.host.callbacks.lock_current(
            self.link,
            self.callback.id,
            chat_id=self.callback.chat_id,
            message_id=self.callback.message_id,
            message_thread_id=self.callback.message_thread_id,
            guard=self.base,
            session=writer,
        )
        selected = group.selected
        if selected.kind != "delivery_retry" or selected.target_kind != "outbox" or not selected.target_id or selected.target_id != self.callback.target_id or selected.payload.get("outbox_id", selected.target_id) != selected.target_id:
            raise CallbackRefused()
        parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, populate_existing=True)
        target = await writer.get(TelegramOutboxRow, self.callback.target_id, populate_existing=True)
        require_retry_notice(self.link, parent, target, group.rows, self.callback.id)
        if self.group is None:
            self.group, self.parent = group, _item(parent)
        elif group != self.group or _item(parent) != self.parent:
            raise CallbackRefused()
        return self.link

    def projection(self):
        if self.group is None:
            raise CallbackRefused()
        return _RetryProjection(self)


class _RetryProjection:
    def __init__(self, guard):
        self.guard = guard

    def extend(self, query):
        self.column = len(query.column_descriptions)
        parent, control, target = aliased(TelegramOutboxRow), aliased(TelegramCallbackRow), aliased(TelegramOutboxRow)
        return (
            query.add_columns(parent, control, target)
            .join(parent, parent.id == self.guard.group.anchor_outbox_id)
            .outerjoin(control, control.group_id == self.guard.group.selected.group_id)
            .join(target, target.id == self.guard.callback.target_id)
        )

    def check(self, rows):
        guard = self.guard
        guard.current()
        parent = rows[0][self.column]
        controls = {row[self.column + 1].id: row[self.column + 1] for row in rows if row[self.column + 1] is not None}
        require_retry_notice(guard.link, parent, rows[0][self.column + 2], list(controls.values()), guard.callback.id)
        if _item(parent) != guard.parent or {key: _row(value) for key, value in controls.items()} != {row.id: row for row in guard.group.rows}:
            raise CallbackRefused()
        selected = controls.get(guard.callback.id)
        if selected is None:
            raise CallbackRefused()
        guard.host.callbacks._usable(selected)


def require_retry_notice(link, parent, target, controls, selected_id):
    if parent is None or target is None:
        raise CallbackRefused()
    source = {"kind": "text", "source_ref": {"key": "delivery.failed", "values": {}}}
    if (
        any((row.link_id, row.owner_user_id, row.binding_generation, row.chat_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id) for row in (parent, target))
        or parent.class_ != "reply"
        or parent.state != "sent"
        or type(target.payload.get("failure_version")) is not int
        or target.payload["failure_version"] <= 0
        or parent.dedupe_key != f"delivery-failed:{target.id}:{target.payload['failure_version']}"
        or parent.payload
        != {
            "kind": "delivery_failure_notice",
            "repair_outbox_id": target.id,
            "presentation_version": 0,
            "product_basis": {"origins": [], "chat_id": link.chat_id, "message_thread_id": parent.message_thread_id, "finish_route": False, "parts": [source]},
            "product_keyboard_basis": canonical_keyboard([[{"label": "label.retry_delivery", "kind": "delivery_retry", "target_kind": "outbox", "target_id": target.id, "payload": {"outbox_id": target.id}}]]),
        }
        or len(parent.parts) != 1
    ):
        raise CallbackRefused()
    part = parent.parts[0]
    if (
        part.get("kind") != "text"
        or part.get("source_ref") != source["source_ref"]
        or part.get("state") != "sent"
        or type(parent.message_id) is not int
        or parent.message_id <= 0
        or part.get("message_ids") != [parent.message_id]
        or any(type(value) is not int for value in part.get("message_ids", []))
        or not part.get("sent_at")
        or any(control.group_id != part.get("callback_group_id") or type(control.message_id) is not int or control.message_id != parent.message_id or control.message_thread_id != parent.message_thread_id for control in controls)
    ):
        raise CallbackRefused()
    _validate_rows(parent, controls)
    selected = next((control for control in controls if control.id == selected_id), None)
    if selected is None or selected.kind != "delivery_retry" or selected.target_kind != "outbox" or selected.target_id != target.id:
        raise CallbackRefused()
