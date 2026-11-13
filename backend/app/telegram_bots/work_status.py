"""Source-qualified status reads for one explicitly admitted Work attempt."""

import asyncio
from dataclasses import asdict, dataclass, field
from zoneinfo import ZoneInfo

from sqlalchemy import String, and_, cast, select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.authority import AccessOff, CeilingRefused
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.result_notice_sources import fields
from app.telegram_bots.settings import _snapshot, format_time
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_event_source import event_signature
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_status_events import question, report, snapshot
from app.telegram_bots.work_status_text import STATUS_KEYS as STATUS_KEYS
from app.telegram_bots.work_status_text import render_status as render_status
from app.telegram_bots.work_stop_news import NewsClock
from deerflow.agent_instances.contract import AgentConflict, AgentDenied
from deerflow.agent_instances.work_contract import WorkCommand
from deerflow.agent_instances.work_execution_contract import ActivateWork
from deerflow.persistence.agent_instances.human_input import HumanInputEventRow, HumanInputRequestRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.run.model import RunRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.persistence.user.model import UserRow
from deerflow.utils.file_io import await_drained


@dataclass(frozen=True)
class WorkStatusRead:
    key: str
    values: dict
    protected: bool
    terminal: bool
    attempt_id: str
    origin_signature: str
    signature: str
    event_proofs: dict = field(default_factory=dict)
    activation_act_id: str | None = None


class WorkStatusReader:
    def __init__(self, reader, *, settings, clock):
        self.reader, self.settings, self.clock = reader, settings, clock

    async def prepare(self, link):
        settings = await self.settings.get(link.owner_user_id)
        if settings is None:
            raise ProductSourceRetired()
        return NewsClock(settings, await asyncio.to_thread(ZoneInfo, settings.timezone), self.clock)

    async def read(self, link, item):
        async def read():
            projection = StatusProjection(link, item, await self.prepare(link))
            admission = await prepare_attention(self.reader, link)
            await admission(projection=projection)
            return projection.source

        try:
            return await await_drained(read())
        except (AccessOff, CeilingRefused, AgentConflict, AgentDenied):
            raise ProductSourceRetired() from None


class StatusProjection:
    def __init__(self, link, item, clock, source=None):
        self.link, self.item, self.clock, self.source = link, item, clock, source

    def extend(self, query):
        self.column = len(query.column_descriptions)
        work, attempt, event, delivery = AgentWorkRow, AgentWorkAttemptRow, AgentWorkEventRow, TelegramOutboxRow
        activation, progress, terminal, cause = (aliased(event) for _ in range(4))
        request, recipient, settings = HumanInputRequestRow, aliased(UserRow), aliased(TelegramPersonSettingsRow)
        trigger_request, receipt, previous = aliased(HumanInputRequestRow), aliased(HumanInputEventRow), aliased(event)
        prior_activation = (
            select(previous.operation_id)
            .where(previous.work_id == event.work_id, previous.instance_id == event.instance_id, previous.action == "activate", previous.revision < event.revision)
            .order_by(previous.revision.desc())
            .limit(1)
            .correlate(event)
            .scalar_subquery()
        )

        def latest(actions, *, report=False, human=False):
            candidate = aliased(event)
            conditions = [candidate.work_id == work.id, candidate.instance_id == work.instance_id, candidate.revision > activation.revision, candidate.action.in_(actions)]
            if report:
                conditions.append(candidate.request["attempt_id"].as_string() == attempt.id)
            if human:
                conditions.append(candidate.revision < terminal.revision)
            return select(candidate.id).where(*conditions).order_by(candidate.revision.desc()).limit(1).correlate(work, attempt, activation, terminal).scalar_subquery()

        return (
            query.add_columns(work, attempt, activation, event, delivery, progress, terminal, cause, request, recipient.email, settings, RunRow, trigger_request, receipt, prior_activation, TelegramActRow)
            .join(work, work.id == self.item.work_id)
            .join(attempt, and_(attempt.id == self.item.payload.get("work_event_intent", {}).get("attempt_id"), attempt.work_id == work.id))
            .join(activation, and_(activation.work_id == work.id, activation.operation_id == attempt.activation_id))
            .join(event, event.id == self.item.event_id)
            .join(delivery, delivery.id == self.item.id)
            .outerjoin(progress, progress.id == latest(("progress",), report=True))
            .outerjoin(terminal, terminal.id == latest(("attempt_settled", "attempt_contained"), report=True))
            .outerjoin(cause, cause.id == latest(("stop", "edit", "cancel"), human=True))
            .outerjoin(
                request,
                and_(
                    request.instance_id == work.instance_id,
                    request.work_id == work.id,
                    request.assignment_revision == attempt.assignment_revision,
                    request.basis_id == work.blocker["id"].as_string(),
                    cast(request.basis_revision, String) == cast(work.blocker["revision"].as_string(), String),
                    request.state.in_(("pending", "answered")),
                ),
            )
            .outerjoin(recipient, recipient.id == request.recipient_id)
            .join(settings, settings.owner_user_id == delivery.owner_user_id)
            .outerjoin(RunRow, RunRow.run_id == attempt.run_id)
            .outerjoin(trigger_request, trigger_request.id == delivery.request_id)
            .outerjoin(receipt, and_(receipt.instance_id == event.instance_id, receipt.operation_id == event.operation_id))
            .outerjoin(TelegramActRow, TelegramActRow.id == attempt.activation_id)
        )

    def check(self, rows):
        instance, policy = rows[0][1], rows[0][3]
        work, attempt, activation, event, delivery, progress, terminal, cause, request, person, preferences = rows[0][self.column : self.column + 11]
        run = rows[0][self.column + 11]
        trigger_request, receipt, prior_activation = rows[0][self.column + 12 : self.column + 15]
        act = rows[0][self.column + 15]
        intent = self.item.payload.get("work_event_intent", {})
        trigger_keys = {
            "activate": {"status.starting"},
            "progress": {"status.working"},
            "request_input": {"status.waiting_you", "status.waiting_answer"},
            "revise_input": {"status.waiting_you", "status.waiting_answer"},
            "attempt_uncertain": {"status.checking"},
            "attempt_contained": {"status.ended"},
            "attempt_settled": {"status.finished", "status.stopped", "status.ended"},
        }
        try:
            command = ActivateWork.model_validate(activation.request)
        except ValueError:
            raise ProductSourceRetired() from None
        if (
            type(intent) is not dict
            or set(intent) != {"version", "key", "event_revision", "assignment_revision", "attempt_id", "request_revision", "edit_request_revision", "response_id", "hold_until"}
            or intent.get("key") not in STATUS_KEYS
            or intent.get("key") not in trigger_keys.get(event.action, ())
            or event.action == "activate"
            and event.id != activation.id
            or event.action != "activate"
            and event.actor_kind != "human"
            and event.request.get("attempt_id") != attempt.id
            or type(intent.get("version")) is not int
            or intent["version"] != 1
            or any(type(intent.get(key)) is not int for key in ("event_revision", "assignment_revision"))
            or not same_body(command.model_dump(mode="json", exclude_unset=True), activation.request)
            or not same_body(command.model_dump(mode="json"), attempt.request)
            or (activation.action, activation.instance_id, activation.work_id, activation.actor_kind, activation.actor_id) != ("activate", instance.id, work.id, "human", self.link.owner_user_id)
            or (command.operation_id, command.expected_revision + 1, command.expected_assignment_revision, command.thread_id) != (attempt.activation_id, activation.revision, attempt.assignment_revision, attempt.thread_id)
            or activation.assignment_revision != attempt.assignment_revision
            or attempt.requester_id != self.link.owner_user_id
            or work.instance_id != instance.id
            or (event.instance_id, event.work_id, event.revision, event.assignment_revision) != (instance.id, work.id, intent["event_revision"], intent["assignment_revision"])
            or (delivery.link_id, delivery.owner_user_id, delivery.binding_generation, delivery.chat_id, delivery.instance_id, delivery.class_, delivery.silent)
            != (self.link.id, self.link.owner_user_id, self.link.binding_generation, self.link.chat_id, instance.id, "reply" if self.item.payload.get("work_news_basis", {}).get("reveal") else "progress", True)
            or delivery.dedupe_key != ("progress:" + attempt.id if event.action == "activate" else "edit:" + event.id)
            or _item(delivery) != self.item
            or delivery.state != "sending"
            and any(getattr(delivery, field) is not None for field in ("claim_owner", "claim_id", "claimed_at"))
            or any(type(activation.result.get(key)) is not int for key in ("revision", "assignment_revision"))
            or (activation.result.get("id"), activation.result.get("instance_id"), activation.result.get("revision"), activation.result.get("assignment_revision")) != (work.id, instance.id, activation.revision, attempt.assignment_revision)
        ):
            raise ProductSourceRetired()
        snapshot(event, work, instance)
        if event.action in {"request_input", "revise_input"}:
            question(event, attempt, instance, trigger_request, receipt, prior_activation, intent, self.item)
        elif event.action == "progress":
            command = report(event, attempt, instance)
            if event.result.get("progress") != command.statement:
                raise ProductSourceRetired()
        settings = _snapshot(preferences)
        if (settings.locale, settings.timezone) != (self.clock.settings.locale, self.clock.settings.timezone):
            raise ProductSourceRetired()
        if work.blocker is not None and (
            type(work.blocker) is not dict
            or type(work.blocker.get("revision")) is not int
            or not 1 <= work.blocker["revision"] <= 2147483647
            or type(work.blocker.get("assignment_revision")) is not int
            or work.blocker["assignment_revision"] != work.assignment_revision
        ):
            raise ProductSourceRetired()
        if work.blocker is not None and work.assignment_revision == attempt.assignment_revision and request is None:
            raise ProductSourceRetired()
        note = ""
        if progress is not None:
            snapshot(progress, work, instance)
            command = report(progress, attempt, instance)
            if progress.result.get("progress") != command.statement:
                raise ProductSourceRetired()
            note = command.statement
        ended = attempt.status in {"succeeded", "failed", "cancelled"}
        if run is not None and (
            type(run.metadata_json) is not dict
            or run.operation_kind != "run"
            or (run.run_id, run.thread_id, run.user_id, run.idempotency_key) != (attempt.run_id, attempt.thread_id, attempt.requester_id, "agent-work:" + attempt.id)
            or (run.metadata_json.get("agent_instance_id"), run.metadata_json.get("agent_work_id"), run.metadata_json.get("agent_work_attempt_id")) != (instance.id, work.id, attempt.id)
        ):
            raise ProductSourceRetired()
        time = attempt.created_at
        if ended:
            if terminal is None or attempt.settled_at is None:
                raise ProductSourceRetired()
            expected = {"attempt_id": attempt.id, "run_id": attempt.run_id, "status": attempt.status}
            if terminal.action == "attempt_contained":
                expected.update(reason=attempt.contained_reason, attachment_id=attempt.attachment_id)
                if terminal.actor_kind != "host" or not terminal.actor_id:
                    raise ProductSourceRetired()
            elif (terminal.actor_kind, terminal.actor_id) != ("nonhuman", instance.principal_id):
                raise ProductSourceRetired()
            if terminal.action == "attempt_settled" and (
                attempt.run_id is None or run is not None and run.status not in {"succeeded": {"success"}, "failed": {"error", "timeout"}, "cancelled": {"interrupted", "error", "timeout", "success"}}[attempt.status]
            ):
                raise ProductSourceRetired()
            if (
                any(type(terminal.result.get(key)) is not int for key in ("revision", "assignment_revision"))
                or not same_body(terminal.request, expected)
                or (terminal.result.get("id"), terminal.result.get("instance_id"), terminal.result.get("revision"), terminal.result.get("assignment_revision")) != (work.id, instance.id, terminal.revision, terminal.assignment_revision)
            ):
                raise ProductSourceRetired()
            key, time = "status.ended", attempt.settled_at
            if attempt.status == "succeeded" and terminal.result.get("status") in {"completed", "submitted"}:
                outcome = terminal.result.get("outcome") or {}
                if outcome.get("attempt_id") != attempt.id or not same_body(outcome, attempt.candidate):
                    raise ProductSourceRetired()
                key = "status.finished"
            if attempt.status == "cancelled" and cause is not None:
                try:
                    stop = WorkCommand.model_validate(cause.request)
                except ValueError:
                    raise ProductSourceRetired() from None
                if (
                    not same_body(stop.model_dump(mode="json", exclude_unset=True), cause.request)
                    or stop.operation_id != cause.operation_id
                    or stop.action != cause.action
                    or stop.expected_revision + 1 != cause.revision
                    or stop.expected_assignment_revision + (cause.action in {"edit", "cancel"}) != cause.assignment_revision
                    or any(type(cause.result.get(field)) is not int for field in ("revision", "assignment_revision"))
                    or (cause.result.get("id"), cause.result.get("instance_id"), cause.result.get("revision"), cause.result.get("assignment_revision")) != (work.id, instance.id, cause.revision, cause.assignment_revision)
                    or cause.actor_kind != "human"
                ):
                    raise ProductSourceRetired()
                if cause.action == "stop" and stop.attempt_id == attempt.id and cause.actor_id == self.link.owner_user_id:
                    key = "status.stopped"
        elif attempt.status == "uncertain":
            key = "status.checking"
        elif attempt.status == "stopping":
            key = "status.stopping"
        elif request is not None and request.state == "pending":
            key = "status.waiting_you" if request.recipient_id == self.link.owner_user_id else "status.waiting_answer"
        else:
            key = "status.starting" if attempt.workspace_ready_at is None and not note else "status.working"
        origin = digest([event_signature(activation, None, {}), fields(attempt, ("id", "work_id", "activation_id", "requester_id", "assignment_revision", "thread_id", "incarnation", "request")), aware(attempt.created_at).isoformat()])
        values = {"objective": activation.result["objective"], "name": instance.name, "progress": note, "person": person, "time": format_time(aware(time), settings=settings, now=self.clock.now(), zone=self.clock.zone)}
        data = dict(key=key, values=values, protected=instance.custody != "personal" and policy == "protected", terminal=ended, attempt_id=attempt.id, origin_signature=origin)
        if act is not None and (act.link_id, act.owner_user_id, act.bot_id, act.binding_generation, act.telegram_user_id, act.chat_id) == (
            self.link.id,
            self.link.owner_user_id,
            self.link.bot_id,
            self.link.binding_generation,
            self.link.telegram_user_id,
            self.link.chat_id,
        ):
            if (act.kind, act.instance_id, act.work_id, act.request_id, act.message_id) != ("activate", instance.id, work.id, None, None) or act.attempts < 1 or not same_body(act.body, activation.request):
                raise ProductSourceRetired()
            data["activation_act_id"] = act.id
        proofs = {fact.id: digest([event_signature(fact, None, {}), aware(fact.created_at).isoformat()]) for fact in (activation, event, progress, terminal, cause) if fact is not None}
        fresh = WorkStatusRead(
            **data,
            event_proofs=proofs,
            signature=digest(
                [
                    data,
                    proofs,
                    event_signature(event, receipt, intent),
                    fields(attempt, ("status", "run_id", "contained_reason", "attachment_id")),
                    fields(run, ("run_id", "thread_id", "user_id", "status", "idempotency_key", "metadata_json")),
                    fields(work, ("revision", "assignment_revision")),
                ]
            ),
        )
        if self.source is not None and not same_body(asdict(self.source), asdict(fresh)):
            raise ProductSourceRetired()
        self.source = fresh
