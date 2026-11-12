"""Stopped notices use the canonical settlement and intervening human command."""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import aliased

from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.result_notice_sources import fields
from app.telegram_bots.settings import _snapshot, format_time
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_event_source import event_signature
from app.telegram_bots.work_result_identity import digest
from deerflow.agent_instances.work_contract import WorkCommand
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramPersonSettingsRow
from deerflow.persistence.user.model import UserRow

STOP_KEYS = {"attempt.stopped": "stop", "attempt.stopped_changed": "edit", "attempt.stopped_withdrawn": "cancel"}


@dataclass(frozen=True)
class NewsClock:
    settings: object
    zone: ZoneInfo
    now: Callable[[], datetime]


async def prepare_news_clock(reader, link, item):
    if item.payload.get("work_event_intent", {}).get("key") != "attempt.stopped":
        return None
    async with reader.links.session_factory() as session:
        row = await session.get(TelegramPersonSettingsRow, link.owner_user_id)
        if row is None:
            raise ProductSourceRetired()
        settings = _snapshot(row)
    zone = await asyncio.to_thread(ZoneInfo, settings.timezone)
    return NewsClock(settings, zone, reader.clock)


class StopNews:
    def __init__(self, link, item, clock):
        self.link, self.item, self.clock = link, item, clock

    def extend(self, query, activation):
        self.column = len(query.column_descriptions)
        event = AgentWorkEventRow
        cause, candidate, actor, settings = aliased(event), aliased(event), aliased(UserRow), aliased(TelegramPersonSettingsRow)
        prior = (
            select(candidate.id)
            .where(candidate.work_id == event.work_id, candidate.instance_id == event.instance_id, candidate.revision > activation.revision, candidate.revision < event.revision, candidate.action.in_(("stop", "edit", "cancel")))
            .order_by(candidate.revision.desc())
            .limit(1)
            .correlate(event, activation)
            .scalar_subquery()
        )
        return query.add_columns(cause, actor.email, settings).outerjoin(cause, cause.id == prior).outerjoin(actor, actor.id == cause.actor_id).join(settings, settings.owner_user_id == TelegramBotLinkRow.owner_user_id)

    def qualify(self, rows, event, work, instance, attempt, activation, intent):
        cause, person, settings = rows[0][self.column : self.column + 3]
        try:
            if cause is None or attempt is None or activation is None or cause.action == "stop" and self.clock is None:
                raise ValueError()
            command = WorkCommand.model_validate(cause.request)
            if (
                not same_body(command.model_dump(mode="json", exclude_unset=True), cause.request)
                or command.action != STOP_KEYS[intent["key"]]
                or cause.action != command.action
                or command.operation_id != cause.operation_id
                or command.expected_revision + 1 != cause.revision
                or command.expected_assignment_revision + (cause.action in {"edit", "cancel"}) != cause.assignment_revision
                or type(cause.result.get("revision")) is not int
                or cause.result["revision"] != cause.revision
                or type(cause.result.get("assignment_revision")) is not int
                or cause.result["assignment_revision"] != cause.assignment_revision
                or (cause.result.get("id"), cause.result.get("instance_id"), cause.result.get("creator_id")) != (work.id, work.instance_id, work.creator_id)
                or cause.actor_kind != "human"
                or cause.actor_id == self.link.owner_user_id
                or cause.action == "stop"
                and command.attempt_id != attempt.id
                or not same_body(event.request, {"attempt_id": attempt.id, "run_id": attempt.run_id, "status": "cancelled"})
                or event.action != "attempt_settled"
                or (event.actor_kind, event.actor_id) != ("nonhuman", instance.principal_id)
                or attempt.status != "cancelled"
                or attempt.settled_at is None
                or attempt.run_id is None
                or (attempt.work_id, attempt.activation_id, attempt.requester_id) != (work.id, activation.operation_id, self.link.owner_user_id)
                or (activation.instance_id, activation.work_id, activation.action) != (event.instance_id, work.id, "activate")
                or cause.action == "stop"
                and (settings.timezone != self.clock.settings.timezone or settings.locale != self.clock.settings.locale)
            ):
                raise ValueError()
        except (ValueError, TypeError, KeyError):
            raise ProductSourceRetired() from None
        values = {"person": person}
        if cause.action == "stop":
            now = self.clock.now()
            values.update(
                time=format_time(aware(cause.created_at), self.clock.settings, now=now, zone=self.clock.zone),
                time_zone=settings.timezone,
                time_locale=settings.locale,
                time_date=now.astimezone(self.clock.zone).date().isoformat(),
            )
        signature = digest([event_signature(event, None, intent), event_signature(cause, None, {}), aware(cause.created_at).isoformat(), fields(attempt, ("id", "work_id", "activation_id", "requester_id", "run_id", "status"))])
        return signature, values
