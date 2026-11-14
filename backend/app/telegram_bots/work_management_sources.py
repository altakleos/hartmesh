"""Saved-work card sources join immutable drafts, Work and actual gestures."""

from dataclasses import asdict
from datetime import datetime, timedelta

from sqlalchemy import and_, false
from sqlalchemy.orm import aliased

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.assignment_sources import private_facts, private_snapshot
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.work_actions import same_body, thaw
from app.telegram_bots.work_details import WorkDetailProjection
from app.telegram_bots.work_edit_drafts import snapshot_edit
from app.telegram_bots.work_management_navigation import ManagementNavigationProof, navigation_for
from app.telegram_bots.work_management_origins import ManagementOpening
from app.telegram_bots.work_management_recovery import ManagementRecoveryProof, recovery_for
from app.telegram_bots.work_news_sources import WorkNewsRead
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_view_sources import WorkViewReader, detail_read
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.utils.file_io import await_drained


def management_intent(item):
    value = item.payload.get("work_management_intent")
    if (
        type(value) is not dict
        or set(value) != {"version", "kind", "work_id", "draft_id", "expected_revision", "expected_assignment_revision", "origin", "act_id"}
        or type(value["version"]) is not int
        or value["version"] != 1
        or value["kind"] not in {"edit", "cancel"}
        or any(type(value[key]) is not int or value[key] < 1 for key in ("expected_revision", "expected_assignment_revision"))
        or type(value["work_id"]) is not str
        or len(value["work_id"]) != 32
        or (value["draft_id"] is None) != (value["kind"] == "cancel")
        or value["draft_id"] is not None
        and (type(value["draft_id"]) is not str or len(value["draft_id"]) != 32)
        or value["act_id"] is not None
        and (type(value["act_id"]) is not str or len(value["act_id"]) != 32)
    ):
        raise ProductSourceRetired()
    return value


def management_source(link, item, intent, detail, root, act, event, *, instance, clock, config):
    expires = aware(item.created_at) + timedelta(hours=config.callbacks.draft_ttl_hours)
    phase = "draft" if root is not None else "confirm"
    if root is not None:
        snapshot_edit(link, root)
        if act is None and set(root.continuation) != {"edit_draft_source"}:
            raise ProductSourceRetired()
        expires = aware(root.created_at) + timedelta(hours=config.callbacks.draft_ttl_hours)
        if list(root.operation_ids) != ([act.id] if act is not None else []):
            raise ProductSourceRetired()
        if (root.work_id, root.body["expected_revision"], root.body["expected_assignment_revision"]) != (intent["work_id"], intent["expected_revision"], intent["expected_assignment_revision"]):
            raise ProductSourceRetired()
        if root.state == "refused":
            phase = {"discarded": "discarded", "replaced": "replaced", "draft-expired": "expired"}.get(root.reason)
            if phase is None:
                raise ProductSourceRetired()
    if act is not None:
        # The dedicated consent owner qualifies the actual immutable command.
        from app.telegram_bots.work_management_consent import management_receipt, require_management_scope

        require_management_scope(link, act, intent)
        if act.state == "done":
            management_receipt(act, event)
            phase = "saved" if act.kind == "edit" else "withdrawn"
            if (
                detail.record["assignment_revision"] != event.assignment_revision
                or any(not same_body(detail.record.get(key), event.result.get(key)) for key in ("objective", "success_criteria", "due_at"))
                or act.kind == "cancel"
                and detail.record["status"] != "cancelled"
            ):
                phase = "changed"
        elif act.state == "refused" and act.reason == "unchanged":
            phase = "unchanged"
        elif act.state in {"refused", "conflict"}:
            phase = "changed" if act.state == "conflict" else "refused"
        else:
            phase = "saving"
        if act.cancel_requested_at is not None and act.state != "done":
            phase = "cancelled"
    elif clock.now() >= expires:
        phase = "expired"
    elif (detail.record["revision"], detail.record["assignment_revision"]) != (intent["expected_revision"], intent["expected_assignment_revision"]):
        phase = "changed"

    navigation = navigation_for(item)
    if navigation is not None:
        phase = "discarded" if navigation["kind"] == "discard" else "back"

    def date(value):
        return datetime.fromisoformat(value).astimezone(clock.zone).strftime("%Y-%m-%d %H:%M") if value else None

    draft = root.body["draft"] if root is not None else None
    data = {
        "intent": intent,
        "phase": phase,
        "draft_id": intent["draft_id"],
        "draft": thaw(draft),
        "root": private_facts(root) if root is not None else None,
        "act": private_facts(act) if act is not None else None,
        "detail": asdict(detail),
        "detail_dates": {"date": date(detail.record["created_at"]), "due": date(detail.record["due_at"])},
        "objective": detail.record["objective"],
        "running": intent.get("act_id") is not None and act.stop_dispatch is not None or detail.record.get("attempt") is not None and detail.record["attempt"]["status"] in {"starting", "running", "stopping", "uncertain"},
        "expires_at": expires.isoformat(),
        "timezone": clock.settings.timezone,
        "navigation": navigation,
        "notice": management_notice(act, now=clock.now(), config=config),
        "recovery": recovery_for(item),
    }
    values = {"name": instance.name, "due": date(draft["due_at"]) if draft is not None else None}
    data = thaw(data)
    return WorkNewsRead(
        intent["work_id"],
        "management.edit" if intent["kind"] == "edit" else "management.withdraw",
        values,
        detail.protected,
        data,
        digest([link.id, link.binding_generation, {key: value for key, value in intent.items() if key != "act_id"}]),
        digest([data, values]),
    )


def management_notice(act, *, now, config):
    notice = {"key": None, "cancel": False, "retry": False}
    if act is None:
        return notice
    if act.state == "done":
        if act.stop_dispatch is not None and (act.stop_dispatch["state"] == "uncertain" or act.stop_dispatch["state"] == "ended" and act.stop_dispatch.get("ended_from") in {"ready", "uncertain"}):
            notice.update(key="work.stop_unconfirmed", retry=True)
    elif act.cancel_requested_at is not None:
        notice["key"] = "work.action_cancelled"
    elif act.state in {"pending", "uncertain"}:
        automatic = act.state == "pending" and act.reason is None and act.attempts < config.acts.max_attempts and act.first_attempt_at is not None and now < aware(act.first_attempt_at) + timedelta(hours=1)
        notice.update(key="work.action_waiting" if automatic else "act.unconfirmed", cancel=automatic, retry=not automatic)
    return notice


class WorkManagementReader(WorkViewReader):
    def __init__(self, acts):
        super().__init__(work=acts.work, authority=acts.authority, links=acts.links, clock=acts.outbox.clock)
        self.acts, self.config = acts, acts.outbox.config

    async def read(self, link, item):
        async def perform():
            intent = management_intent(item)
            detail = await self.detail(link, intent["work_id"])
            clock = await self.prepare_clock(link, item)
            async with self.links.session_factory() as reader:
                raw = await reader.get(TelegramActRow, intent["act_id"]) if intent["act_id"] else None
                act = _snapshot(raw) if raw is not None else None
            projection = WorkManagementProjection(self, link, item, detail=detail, act=act, clock=clock)
            current = await prepare_attention(self, link)
            await current(projection=projection)
            return projection.source

        return await await_drained(perform())


class WorkManagementProjection:
    def __init__(self, reader, link, item, source=None, *, detail=None, act=None, clock):
        self.reader, self.link, self.item, self.expected, self.clock = reader, link, item, source, clock
        self.intent = management_intent(item)
        detail = detail_read(source.work["detail"]) if source is not None else detail
        if detail is None:
            raise ProductSourceRetired()
        self.detail = WorkDetailProjection(reader, link, detail.record, detail.request, reusable=detail.can_reuse_conversation, expected=detail)
        self.opening, self.source = ManagementOpening(link, item, self.intent), None
        self.navigation = ManagementNavigationProof(link, item)
        self.recovery = ManagementRecoveryProof(link, item)
        self.act = private_snapshot(source.work["act"]) if source is not None and source.work["act"] is not None else act
        if (self.act is None) != (self.intent["act_id"] is None):
            raise ProductSourceRetired()
        self.consent = None
        if self.act is not None:
            from app.telegram_bots.work_management_consent import ManagementConsent

            self.consent = ManagementConsent(self.act)

    def extend(self, query):
        result = self.detail.extend(query)
        self.column = len(result.column_descriptions)
        self.delivery_column = self.column + 3
        root, act, event, delivery, settings = aliased(TelegramActRow), aliased(TelegramActRow), aliased(AgentWorkEventRow), aliased(TelegramOutboxRow), aliased(TelegramPersonSettingsRow)
        result = (
            result.add_columns(root, act, event, delivery, settings)
            .outerjoin(root, root.id == self.intent["draft_id"] if self.intent["draft_id"] else false())
            .outerjoin(act, act.id == self.intent["act_id"] if self.intent["act_id"] else false())
            .outerjoin(event, and_(event.instance_id == self.link.agent_ref, event.operation_id == self.intent["act_id"]) if self.intent["act_id"] else false())
            .join(delivery, delivery.id == self.item.id)
            .join(settings, settings.owner_user_id == self.link.owner_user_id)
        )
        result = self.opening.extend(result)
        result = self.navigation.extend(result)
        result = self.recovery.extend(result)
        self.reservation_column = len(result.column_descriptions)
        reservation, control = aliased(TelegramActRow), aliased(TelegramCallbackRow)
        result = (
            result.add_columns(reservation, control)
            .outerjoin(reservation, and_(reservation.kind.in_(("edit", "cancel")), reservation.continuation["management_origin"]["anchor_outbox_id"].as_string() == self.item.id))
            .outerjoin(control, and_(control.payload["anchor_outbox_id"].as_string() == self.item.id, control.kind.in_(("work_change_save", "work_withdraw_confirm"))))
        )
        return self.consent.extend(result) if self.consent is not None else result

    def check(self, rows):
        raise ProductSourceRetired()

    def check_with_permissions(self, rows, held):
        self.detail.check_with_permissions(rows, held)
        self.opening.check(rows)
        if self.consent is not None:
            self.consent.check(rows, allow_closed=True)
        root, act, event, delivery, settings = rows[0][self.column : self.column + 5]
        self.navigation.check(rows, delivery, root)
        self.recovery.check(rows, delivery, act)
        expected = {self.intent["act_id"]} if self.intent["act_id"] is not None else set()
        reserved = {row[self.reservation_column].id for row in rows if row[self.reservation_column] is not None}
        markers = {value for row in rows if row[self.reservation_column + 1] is not None and (value := row[self.reservation_column + 1].payload.get("work_action_id")) is not None}
        if reserved != expected or markers != expected:
            raise ProductSourceRetired()
        if (
            rows[0][2] & 6 != 6
            or "agents:write" not in held
            or _item(delivery) != self.item
            or (delivery.link_id, delivery.owner_user_id, delivery.binding_generation, delivery.chat_id, delivery.instance_id, delivery.work_id, delivery.class_, delivery.request_id, delivery.event_id)
            != (self.link.id, self.link.owner_user_id, self.link.binding_generation, self.link.chat_id, self.link.agent_ref, self.intent["work_id"], "reply", None, None)
            or (act is None) != (self.intent["act_id"] is None)
            or (root is None) != (self.intent["draft_id"] is None)
            or settings.timezone != self.clock.settings.timezone
            or act is not None
            and not same_body(private_facts(act), private_facts(self.act))
        ):
            raise ProductSourceRetired()
        source = management_source(
            self.link, self.item, self.intent, self.detail.source, _snapshot(root) if root is not None else None, _snapshot(act) if act is not None else None, event, instance=rows[0][1], clock=self.clock, config=self.reader.config
        )
        if self.expected is not None and not same_body(asdict(self.expected), asdict(source)):
            raise ProductSourceRetired()
        self.source = source
