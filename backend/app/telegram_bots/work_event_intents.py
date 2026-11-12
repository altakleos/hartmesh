"""Recipient-specific Work delivery intents, without content or transport.

Called only in WorkEventReader's locked pair transaction. Rendering, exact edit
anchors, current delivery authority and supervisor installation remain separate.
"""

from datetime import timedelta
from uuid import uuid4

from sqlalchemy import select

from app.telegram_bots.notifications import aware
from app.telegram_bots.work_event_facts import event_attempt, event_request, event_response
from deerflow.agent_instances.work import DELEGATE, INSPECT, MANAGE, UNRESOLVED
from deerflow.persistence.agent_instances.human_input import HumanInputEventRow
from deerflow.persistence.agent_instances.model import AgentInstanceRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramOutboxRow, TelegramPersonSettingsRow

_QUIET_KEYS = frozenset({"result.waiting_review", "attempt.stopped", "attempt.stopped_changed", "attempt.stopped_withdrawn", "request.answered", "result.accepted", "work.changed", "work.reopened"})
_IGNORED = frozenset({"delegate", "outcome", "suggest", "stop", "derive", "input", "reconcile_mandate", "reconcile_attempt"})


class WorkEventIntents:
    def __init__(self, *, links, human):
        self.links, self.human = links, human
        if links.session_factory.kw.get("bind") is not human._sf.kw.get("bind"):
            raise ValueError("work-event-shared-storage-required")

    async def project(self, writer, link, event):
        if event.action in _IGNORED:
            return
        parent = await writer.get(AgentInstanceRow, event.instance_id)
        if parent is None or link.agent_ref != event.instance_id:
            raise ValueError("work-event-instance-scope")
        if not await self.human._eligible(writer, parent, link.owner_user_id, INSPECT):
            return
        settings = await writer.get(TelegramPersonSettingsRow, link.owner_user_id)
        if settings is None:
            raise ValueError("work-event-settings-missing")
        attempt = await event_attempt(writer, event)
        request, receipt = await event_request(writer, event)
        context = _Projection(self, writer, link, event, parent, settings, attempt, request, receipt)
        await context.write()


class _Projection:
    def __init__(self, owner, writer, link, event, parent, settings, attempt, request, receipt):
        self.owner, self.writer, self.link, self.event = owner, writer, link, event
        self.parent, self.settings, self.attempt, self.request, self.receipt = parent, settings, attempt, request, receipt
        self.person = link.owner_user_id
        self.requester = attempt.requester_id if attempt else None
        self.creator = event.result.get("creator_id")

    async def eligible(self, person, permission):
        return await self.owner.human._eligible(self.writer, self.parent, person, permission)

    def request_revision(self):
        if self.receipt is not None:
            return self.receipt.receipt.get("request_revision")
        if self.event.action == "assess_input":
            return (self.event.request.get("request_basis") or {}).get("request_revision")
        if self.event.action in {"request_input", "revise_input", "attempt_settled"} and self.request is not None:
            return 1
        return None

    async def save(self, key, class_, *, recipient=None, actor=False, edit=False, response=None, hold=False, edit_request_revision=None):
        if recipient != self.person or not actor and self.event.actor_kind == "human" and self.event.actor_id == self.person:
            return
        if class_ in {"question", "reminder"} and self.settings.notify not in {"all", "questions"} or class_ in {"result", "interrupt", "info"} and self.settings.notify != "all":
            return
        event, link = self.event, self.link
        # A settlement may both ask a manager to route its new review and tell
        # that same person, as requester, that the result awaits review.
        dedupe = "edit:" + event.id if edit else "event:" + event.id + ":" + class_
        if class_ == "progress" and event.action == "activate":
            dedupe = "progress:" + self.attempt.id
        if class_ == "reminder":
            dedupe = "remind:" + response.id
        existing = await self.writer.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == dedupe))
        if existing is not None:
            return
        created = aware(event.created_at)
        intent = {
            "version": 1,
            "key": key,
            "event_revision": event.revision,
            "assignment_revision": event.assignment_revision,
            "attempt_id": self.attempt.id if self.attempt else None,
            "request_revision": self.request_revision(),
            "edit_request_revision": edit_request_revision,
            "response_id": response.id if response else None,
            "hold_until": (created + timedelta(seconds=self.owner.links.config.outbox.decision_hold_seconds)).isoformat() if hold else None,
        }
        self.writer.add(
            TelegramOutboxRow(
                id=uuid4().hex,
                link_id=link.id,
                owner_user_id=self.person,
                dedupe_key=dedupe,
                class_=class_,
                binding_generation=link.binding_generation,
                instance_id=event.instance_id,
                work_id=event.work_id,
                request_id=self.request.id if self.request else None,
                event_id=event.id,
                payload={"work_event_intent": intent, "presentation_version": 0},
                parts=[],
                state="pending",
                not_before=aware(response.created_at) + timedelta(minutes=60) if class_ == "reminder" else created,
                silent=True if edit or class_ in {"progress", "reminder"} or key in _QUIET_KEYS else None,
                chat_id=link.chat_id,
                created_at=created,
            )
        )
        await self.writer.flush()

    async def question_edit(self, key):
        if self.request is None:
            return
        revision = self.event.request.get("expected_request_revision") if self.event.action == "request_route" else self.request_revision()
        if type(revision) is not int or revision < 1:
            raise ValueError("work-event-edit-revision")
        candidates = await self.writer.execute(
            select(TelegramOutboxRow.payload["work_event_intent"], TelegramOutboxRow.payload["work_request_basis"]["source"]["seen"]).where(
                TelegramOutboxRow.link_id == self.link.id,
                TelegramOutboxRow.binding_generation == self.link.binding_generation,
                TelegramOutboxRow.request_id == self.request.id,
                TelegramOutboxRow.class_ == "question",
                ~TelegramOutboxRow.dedupe_key.startswith("edit:"),
            )
        )
        for intent, shown in candidates:
            basis = intent if type(intent) is dict else shown
            if type(basis) is dict and type(basis.get("request_revision")) is int and type(basis.get("assignment_revision")) is int and (basis["request_revision"], basis["assignment_revision"]) == (revision, self.event.assignment_revision):
                await self.save(key, "question", recipient=self.person, actor=True, edit=True, edit_request_revision=revision)
                return

    async def progress(self, key):
        await self.save(key, "progress", recipient=self.requester, actor=True, edit=True)

    async def question(self, *, routed=False, response=None):
        req = self.request
        if req is None:
            raise ValueError("work-event-question-missing")
        if req.state != "pending":
            return
        later_route = await self.writer.scalar(
            select(HumanInputEventRow.id)
            .join(AgentWorkEventRow, (AgentWorkEventRow.instance_id == HumanInputEventRow.instance_id) & (AgentWorkEventRow.operation_id == HumanInputEventRow.operation_id))
            .where(HumanInputEventRow.request_id == req.id, HumanInputEventRow.action == "route", AgentWorkEventRow.work_id == self.event.work_id, AgentWorkEventRow.revision > self.event.revision)
            .limit(1)
        )
        if later_route is not None:
            return
        can_answer = await self.eligible(req.recipient_id, DELEGATE if req.purpose == "information" else MANAGE)
        if response is not None or not can_answer:
            if await self.eligible(self.person, MANAGE):
                await self.save("request.unrouted", "question", recipient=self.person, response=response, hold=True)
        elif req.purpose != "review" or self.event.action == "attempt_settled" or routed:
            key = {"information": "ask.info", "decision": "ask.decision", "review": "review.ready"}[req.purpose]
            await self.save(key, "question", recipient=req.recipient_id, actor=routed, hold=req.purpose == "decision")

    async def write(self):
        action = self.event.action
        if action in {"request_input", "revise_input"}:
            await self.question()
            if self.request.purpose == "information":
                await self.progress("status.waiting_you" if self.request.recipient_id == self.requester else "status.waiting_answer")
        elif action == "request_route":
            await self.question_edit("request.rerouted")
            await self.question(routed=True)
        elif action == "request_withdraw":
            await self.question_edit("request.withdrawn")
        elif action == "assess_input":
            await self.question_edit("request.assessed")
        elif action == "human_response":
            await self.response()
        elif action == "activate":
            await self.save("status.starting", "progress", recipient=self.requester, actor=True)
        elif action == "progress":
            await self.progress("status.working")
        elif action == "attempt_uncertain":
            await self.progress("status.checking")
        elif action == "attempt_contained":
            await self.progress("status.ended")
            await self.save("interrupted", "interrupt", recipient=self.requester)
        elif action == "attempt_settled":
            await self.settled()
        elif action in {"decide", "changes_requested", "derived"}:
            await self.save({"decide": "decision.recorded", "changes_requested": "result.changes", "derived": "work.proposed"}[action], "info", recipient=self.requester)
        elif action in {"accept", "cancel"}:
            key = "result.accepted" if action == "accept" else "work.cancelled"
            if self.person in {self.requester, self.creator}:
                await self.save(key, "info", recipient=self.person)
        elif action in {"edit", "reopen"}:
            await self.save("work.changed" if action == "edit" else "work.reopened", "info", recipient=self.creator)

    async def response(self):
        response = await event_response(self.writer, self.event, self.request, self.receipt)
        if response.disposition != "supplied":
            await self.question(response=response)
            return
        if self.request.purpose != "information":
            return
        act = await self.writer.get(TelegramActRow, self.event.operation_id)
        own_telegram = act is not None and (act.kind, act.link_id, act.instance_id, act.work_id, act.request_id, act.owner_user_id) == (
            "respond",
            self.link.id,
            self.event.instance_id,
            self.event.work_id,
            self.request.id,
            self.event.actor_id,
        )
        if not own_telegram or self.event.actor_id != self.person:
            await self.question_edit("request.answered_elsewhere" if self.event.actor_id == self.person else "request.answered_by")
        await self.save("request.answered", "info", recipient=self.requester, response=response)
        unresolved = await self.writer.scalar(select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.work_id == self.event.work_id, AgentWorkAttemptRow.status.in_(UNRESOLVED)).limit(1))
        if unresolved is None and await self.eligible(response.actor_id, DELEGATE):
            await self.save("remind.ready_to_resume", "reminder", recipient=response.actor_id, actor=True, response=response)

    async def settled(self):
        status = self.event.request.get("status")
        state = self.event.result.get("status")
        if self.attempt is None:
            raise ValueError("work-event-settlement-attempt")
        if status == "succeeded" and state in {"submitted", "completed"}:
            await self.progress("status.finished")
            if state == "submitted":
                # Review creation currently shares the settlement transaction;
                # it has no separate Work request_input event.
                await self.question()
                if self.requester != self.request.recipient_id:
                    await self.save("result.waiting_review", "result", recipient=self.requester)
            elif self.person in {self.requester, self.creator}:
                await self.save("result.done", "result", recipient=self.person)
        elif status == "succeeded":
            await self.progress("status.ended")
            if state == "open":
                await self.save("attempt.no_result", "interrupt", recipient=self.requester)
        elif status == "failed":
            await self.progress("status.ended")
            await self.save("attempt.failed", "interrupt", recipient=self.requester)
        elif status == "cancelled":
            activation = await self.writer.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.work_id == self.attempt.work_id, AgentWorkEventRow.operation_id == self.attempt.activation_id))
            if activation is None:
                raise ValueError("work-event-cancellation-activation")
            cause = await self.writer.scalar(
                select(AgentWorkEventRow)
                .where(AgentWorkEventRow.work_id == self.event.work_id, AgentWorkEventRow.revision > activation.revision, AgentWorkEventRow.revision < self.event.revision, AgentWorkEventRow.action.in_(("stop", "edit", "cancel")))
                .order_by(AgentWorkEventRow.revision.desc())
                .limit(1)
            )
            own_stop = cause is not None and cause.actor_kind == "human" and cause.actor_id == self.requester
            await self.progress("status.stopped" if own_stop and cause.action == "stop" else "status.ended")
            if cause is None:
                await self.save("attempt.stopped_restart", "interrupt", recipient=self.requester)
            elif not own_stop:
                key = {"stop": "attempt.stopped", "edit": "attempt.stopped_changed", "cancel": "attempt.stopped_withdrawn"}[cause.action]
                await self.save(key, "info", recipient=self.requester)
