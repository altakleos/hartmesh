"""Compose Work-chat entry points around the supervisor's existing owners."""

import asyncio
import logging
from copy import deepcopy

from app.telegram_bots.assignment_answers import AssignmentAnswers
from app.telegram_bots.assignment_commands import AssignmentCommands
from app.telegram_bots.assignment_controls import AssignmentAdmissions
from app.telegram_bots.assignment_delivery import AssignmentPlans
from app.telegram_bots.assignment_inputs import AssignmentInputs
from app.telegram_bots.assignment_navigation import AssignmentDraftNavigation
from app.telegram_bots.assignment_prompt_delivery import AssignmentPromptPlans
from app.telegram_bots.assignment_prompt_sources import AssignmentPromptReader
from app.telegram_bots.assignment_sources import AssignmentReader
from app.telegram_bots.assignment_voice_inputs import AssignmentVoiceInputs
from app.telegram_bots.authority import CeilingRefused
from app.telegram_bots.notification_runtime import _RuntimeGuard
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.voice_inputs import VoiceInputRouter
from app.telegram_bots.work_capacity_offers import WorkCapacityOffers
from app.telegram_bots.work_capacity_reads import WorkCapacityReader
from app.telegram_bots.work_capacity_sources import WorkCapacityOfferReader
from app.telegram_bots.work_command_stops import WorkCommandStops
from app.telegram_bots.work_execution_delivery import WorkExecutionPlans
from app.telegram_bots.work_execution_sources import WorkExecutionReader
from app.telegram_bots.work_management_delivery import WorkManagementPlans
from app.telegram_bots.work_management_editing import WorkManagementEdits
from app.telegram_bots.work_management_sources import WorkManagementReader
from app.telegram_bots.work_management_voice_inputs import ManagementVoiceInputs
from app.telegram_bots.work_news_delivery import WorkNewsSources
from app.telegram_bots.work_start_controls import WorkStartAdmissions
from app.telegram_bots.work_stop_controls import WorkStopAdmissions
from app.telegram_bots.work_view_delivery import WorkViewPlans
from app.telegram_bots.work_view_reviews import WorkViewReviews
from app.telegram_bots.work_view_sources import WorkViewReader
from deerflow.agent_instances.contract import AgentConflict, AgentDenied
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)


class WorkChatRuntime:
    def __init__(self, runtime, work):
        self.runtime, self.host = runtime, runtime.host
        host = self.host
        shared = dict(work=work, authority=host.authority, links=host.links, clock=host.outbox.clock)
        delivery = dict(outbox=host.outbox, callbacks=host.callbacks)
        self.views = WorkViewPlans(
            reader=WorkViewReader(**shared),
            **delivery,
        )
        self.views.reader.acts = host.acts
        host.work_view_sources = WorkNewsSources(self.views)
        self.capacity = WorkCapacityReader(gateway=host.thread_client, **shared)
        self.offers = WorkCapacityOffers(reader=WorkCapacityOfferReader(capacity=self.capacity, **shared), acts=host.acts, guard_factory=self.offer_guard, available=runtime.available, **delivery)
        host.work_capacity_sources = WorkNewsSources(self.offers)
        self.executions = WorkExecutionPlans(reader=WorkExecutionReader(capacity=self.capacity, **shared), acts=host.acts, product=host.product_name, **delivery)
        host.work_execution_sources = WorkNewsSources(self.executions)
        self.starts = WorkStartAdmissions(self.views, host.acts)
        from app.telegram_bots.work_start_review_replies import WorkStartReviewReplies

        self.start_reviews = WorkStartReviewReplies(self)
        self.executions.review_handoffs = self.start_reviews
        from app.telegram_bots.work_saved_refusals import WorkSavedRefusals

        self.saved_refusals = WorkSavedRefusals(self)
        self.executions.fixed_refusals = self.saved_refusals
        host.work_saved_refusals = self.saved_refusals
        self.stops = WorkStopAdmissions(self.views, host.acts)
        self.assignments = AssignmentPlans(reader=AssignmentReader(host.acts), **delivery)
        self.assignments.fixed_refusals = self.saved_refusals
        self.assignment_answers = AssignmentAnswers(self.assignments)
        source = getattr(host, "assignment_proposal_source", None)
        if source is not None:
            from app.telegram_bots.assignment_proposal_plans import AssignmentProposalPlans

            self.proposals = AssignmentProposalPlans(self.assignments, source=source)
            host.publications.assignment_proposal_planner = self.proposals.plan_terminal
        self.assignment_prompts = AssignmentPromptPlans(reader=AssignmentPromptReader(host.acts), **delivery)
        self.assignment_inputs = AssignmentInputs(self.assignment_prompts, self.assignments)
        from app.telegram_bots.result_suggestion_drafts import ResultSuggestionDrafts

        self.suggestion_drafts = ResultSuggestionDrafts(self)
        self.assignment_admissions = AssignmentAdmissions(self.assignments, host.acts)
        self.assignment_navigation = AssignmentDraftNavigation(self.assignments)
        host.assignment_sources = WorkNewsSources(self.assignments)
        host.assignment_prompt_sources = WorkNewsSources(self.assignment_prompts)
        self.management = WorkManagementPlans(reader=WorkManagementReader(host.acts), **delivery)
        self.management_edits = WorkManagementEdits(self.management)
        self.command_stops = WorkCommandStops(self)
        from app.telegram_bots.assignment_closed_prompts import AssignmentClosedPrompts

        self.closed_prompts = AssignmentClosedPrompts(self)
        host.work_callbacks.closed.typing_owner = self.closed_prompts
        host.voice_inputs = VoiceInputRouter(
            host.work_text_inputs.voice,
            {
                "assignment": AssignmentVoiceInputs(self.assignment_inputs, guard_factory=self.guard, closed=self.closed_prompts),
                "management": ManagementVoiceInputs(self.management_edits, guard_factory=self.guard, closed=self.closed_prompts),
            },
        )
        self.detail_plans = {"work_view_intent": self.views, "work_management_intent": self.management, "work_execution_intent": self.executions}
        self.review_views = WorkViewReviews(host.work_callbacks.interactions, self.detail_plans)
        self.assignment_commands = AssignmentCommands(self.assignment_inputs, management=self.management_edits, closed=self.closed_prompts)
        from app.telegram_bots.work_callback_denials import WorkCallbackDenials

        self.denials = WorkCallbackDenials(self)
        host.work_callback_denials = self.denials
        host.work_management_sources = WorkNewsSources(self.management)
        host.work_management_prompt_sources = WorkNewsSources(self.management_edits.prompts)

    async def stop(self):
        await asyncio.gather(self.assignment_inputs.requests.stop(), self.management_edits.stop())

    def materializer(self, item):
        owners = {
            "work_view_intent": self.views,
            "work_execution_intent": self.executions,
            "assignment_intent": self.assignments,
            "assignment_prompt_intent": self.assignment_prompts,
            "work_management_intent": self.management,
            "management_prompt_intent": self.management_edits.prompts,
            "command_stop_intent": self.command_stops.choices,
            "capacity_offer_intent": self.offers,
            "work_start_refusal_intent": self.start_reviews,
        }
        found = [owner for key, owner in owners.items() if key in item.payload]
        if len(found) > 1:
            raise ProductSourceRetired()
        return found[0] if found else None

    def guard(self, channel, link):
        return _RuntimeGuard(self.runtime, self.host.commands._guard(channel, link))

    def offer_guard(self, link):
        channel = self.host.channel(link.id)
        if channel is None:
            raise ProductSourceRetired()
        return self.guard(channel, link)

    async def handle_command(self, channel, update, word):
        link = channel._admission(update).link
        if link.agent_kind != "instance" or word not in {"work", "status", "assign"}:
            return False

        async def perform():
            guard = self.guard(channel, link)
            await guard.prepare()
            if word == "assign":
                await self.assignment_commands.handle(channel, update, link, guard=guard)
                return True
            origin, _created = await self.host.outbox.inbox.accept(link, update, sanitized_input={"command": "work"})
            if origin.route_complete:
                return True
            try:
                await self.views.command(link, origin, guard=guard)
            except AgentDenied:
                if origin.route_kind is not None:
                    raise
                await self.assignment_commands.notice(link, origin, "work.need_inspect", guard=guard)
            return True

        result = await await_drained(perform())
        channel._classified(update, remember=True)
        return result

    async def handle_text(self, channel, update, *, captured_link):
        result = await self.assignment_commands.handle_text(channel, update, captured_link, guard=self.guard(channel, captured_link))
        if result:
            channel._classified(update, remember=True)
        return result

    async def recover_input(self, channel, origin, *, guard):
        from collections.abc import Mapping

        voice = origin.route_basis.get("work_voice")
        if origin.route_kind == "work_card_hold" and isinstance(voice, Mapping) and voice.get("owner") in {"assignment", "management"}:
            return await self.host.voice_inputs.resume(channel, origin, recovery_guard=guard)
        link = guard.captured_link
        if origin.route_kind == "work_card_hold" and isinstance(origin.route_basis.get("closed_typing"), Mapping):
            source = origin.route_basis["closed_typing"]
            await self.closed_prompts.offer(link, origin, source={key: source[key] for key in ("prompt_id", "source_outbox_id", "ambiguous")}, guard=guard)
            return True
        if origin.route_kind == "command_stop":
            return await self.command_stops.recover(link, origin, guard=guard)
        if origin.route_kind == "assignment_hold":
            return await self.assignment_commands.resume(link, origin, guard=guard)
        if origin.route_kind in {"assignment_draft", "assignment_criteria"}:
            await self.assignment_inputs.requests.recover(link, guard=guard)
            if origin.sanitized_input.get("command") == "assign":
                await self.assignment_commands.submit(link, origin, guard=guard)
            else:
                await self.assignment_inputs.answer(link, origin, guard=guard)
            return True
        if origin.route_kind == "management_edit":
            await self.management_edits.recover(link, guard=guard)
            return True
        return False

    async def _unadmitted_lifecycle(self, link, row):
        """Fixed current status may answer a press with no saved effect owner."""
        from sqlalchemy import select

        from app.telegram_bots.callbacks import _row
        from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
        from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow

        notice = getattr(self.host, "notice_instance_unavailable", None)
        if row.kind not in {"work_start", "assign_start", "assign_save"} or notice is None:
            return False
        key, _identity = await self.host.authority.instance_lifecycle(link)
        if key not in {"agent.paused", "agent.retired", "access.agent_off"}:
            return False
        async with self.host.outbox.sessions() as reader:
            selected = await reader.get(TelegramCallbackRow, row.id)
            if (
                selected is None
                or (selected.link_id, selected.binding_generation, selected.chat_id, selected.message_id, selected.message_thread_id, selected.kind, selected.group_id, selected.target_kind, selected.target_id)
                != (link.id, link.binding_generation, link.chat_id, row.message_id, row.message_thread_id, row.kind, row.group_id, row.target_kind, row.target_id)
                or _row(selected).payload != row.payload
                or selected.consumed_at is not None
                or "work_action_id" in selected.payload
            ):
                return False
            members = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == selected.group_id)))
            if not members or any(member.consumed_at is not None or "work_action_id" in member.payload for member in members):
                return False
            if row.kind in {"assign_start", "assign_save"}:
                from app.telegram_bots.assignment_drafts import draft_scope
                from app.telegram_bots.assignment_lineage import require_fresh_draft

                root = await reader.get(TelegramActRow, row.target_id)
                if (
                    root is None
                    or any(getattr(root, key) != value for key, value in draft_scope(link).items())
                    or root.state != "draft"
                    or root.operation_ids
                    or root.continuation is not None
                    or root.stop_dispatch is not None
                    or root.cancel_requested_at is not None
                ):
                    return False
                try:
                    await require_fresh_draft(reader, link, root.id)
                except ProductSourceRetired:
                    return False
            identities = [member.id for member in members]
            owned = await reader.execute(
                select(
                    select(TelegramActRow.id).where(TelegramActRow.id.in_(identities)).exists(),
                    select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.activation_id.in_(identities)).exists(),
                    select(AgentWorkEventRow.id).where(AgentWorkEventRow.operation_id.in_(identities)).exists(),
                )
            )
            if any(owned.one()):
                return False
            self.host.callbacks._unexpired(selected)
        await notice(link)
        return True

    async def execute_callback(self, channel, row, *, query_id=None, update_id=None):
        if row.kind not in {
            "work_details",
            "work_list_page",
            "work_list_open",
            "work_start",
            "stop_attempt",
            "stop_which",
            "assign_save",
            "assign_start",
            "assign_edit",
            "assign_discard",
            "assign_answer",
            "assign_from_suggestion",
            "work_change",
            "work_withdraw",
            "work_change_save",
            "work_withdraw_confirm",
            "back",
            "act_check",
            "cancel_wait",
            "accept_open",
            "changes_open",
        }:
            return False
        link = deepcopy(channel.link)

        async def perform():
            guard = self.guard(channel, link)
            await guard.prepare()
            if await self._unadmitted_lifecycle(link, row):
                return True
            scope = dict(chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard)
            if row.kind == "work_start" and "assignment_revision" not in row.payload:
                await self.views.open(link, row.id, query_id=query_id, refusal=True, **scope)
                return True
            management_parent = None
            if row.kind in {"assign_edit", "assign_discard", "work_change_save", "work_withdraw_confirm", "back", "act_check", "cancel_wait", "accept_open", "changes_open"}:
                from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

                guard.current()
                async with self.host.outbox.sessions() as reader:
                    parent = await reader.get(TelegramOutboxRow, row.payload.get("anchor_outbox_id"))
                    if parent is not None and "work_management_intent" in parent.payload:
                        management_parent = parent.id
                    if row.kind in {"accept_open", "changes_open"} and (parent is None or not self.detail_plans.keys() & parent.payload.keys()):
                        return False
                    if row.kind in {"act_check", "cancel_wait"}:
                        owners = {
                            "work_execution_intent": self.executions,
                            "assignment_intent": self.assignments,
                            "work_management_intent": self.management,
                        }
                        found = [owner for key, owner in owners.items() if parent is not None and key in parent.payload]
                        if not found:
                            return False
                        if len(found) != 1:
                            raise ProductSourceRetired()
                        recovery = found[0]
                if row.kind == "back" and management_parent is None:
                    return False

            def current():
                try:
                    return guard.current() is not False
                except Exception:
                    return False

            if row.kind == "assign_from_suggestion":
                await self.suggestion_drafts.open(link, row.id, **scope)
            elif row.kind == "assign_answer":
                stage = await self.assignment_answers.stage(link, row.id, update_id=update_id, **scope)
                await self.host.turns.accept_staged(channel, stage, admission_current=current)
            elif row.kind == "stop_which":
                await self.command_stops.select(channel, row, link=link, guard=guard)
            elif row.kind in {"accept_open", "changes_open"}:
                await self.review_views.open(link, row.id, **scope)
            elif row.kind in {"act_check", "cancel_wait"}:
                operation = recovery.retry if row.kind == "act_check" else recovery.cancel
                await operation(link, row.id, **scope)
            elif row.kind in {"work_details", "work_list_page", "work_list_open"}:
                await self.views.open(link, row.id, query_id=query_id, **scope)
            elif row.kind in {"work_change", "work_withdraw"}:
                try:
                    await self.management.open(link, row.id, query_id=query_id, **scope)
                except (AgentDenied, CeilingRefused):
                    if await self.denials.offer(link, row, guard=guard, query_id=query_id):
                        return True
                    raise
            elif row.kind == "back":
                await self.management.back(link, row.id, **scope)
            elif management_parent is not None and row.kind == "assign_edit":
                await self.management_edits.open(link, row.id, query_id=query_id, **scope)
            elif management_parent is not None and row.kind == "assign_discard":
                await self.management.discard(link, row.id, **scope)
            elif row.kind in {"work_change_save", "work_withdraw_confirm"}:
                act = await self.management.confirm(link, row.id, **scope)
                try:
                    await self.host.acts.check(act.id, expected={"id": act.id, "kind": act.kind, "body": act.body, "attempts": act.attempts}, attempt_current=current)
                    guard.current()
                    await self.management.observe(link, management_parent, guard=guard)
                except Exception as error:
                    logger.warning("Telegram Work management observation deferred: action=%s cause=%s", act.id, type(error).__name__)
            elif row.kind == "assign_edit":
                await self.assignment_prompts.edit(link, row.id, query_id=query_id, **scope)
            elif row.kind == "assign_discard":
                await self.assignment_navigation.discard(link, row.id, **scope)
            else:
                assignment = row.kind in {"assign_save", "assign_start"}
                controller = self.assignment_admissions if assignment else self.stops if row.kind == "stop_attempt" else self.starts
                try:
                    act = await controller.stage(link, row.id, **scope)
                except (AgentDenied, CeilingRefused):
                    if await self.denials.offer(link, row, guard=guard, query_id=query_id):
                        return True
                    raise
                except AgentConflict as error:
                    if row.kind == "work_start" and error.reason == "awaiting-review":
                        await self.start_reviews.open(link, row.id, query_id=query_id, **scope)
                        return True
                    if row.kind != "work_start" or error.reason not in {"work-changed", "sources-changed"}:
                        raise
                    await self.views.open(link, row.id, query_id=query_id, refusal=True, **scope)
                    return True
                try:
                    await self.host.acts.check(
                        act.id,
                        expected={"id": act.id, "kind": act.kind, "body": act.body, "attempts": act.attempts},
                        attempt_current=current,
                    )
                    guard.current()
                    if assignment:
                        await self.assignments.observe(link, row.target_id, guard=guard)
                        if row.kind == "assign_start":
                            await self.host.acts.resume_continuation(act.id, attempt_current=current)
                            await self.assignments.observe(link, row.target_id, guard=guard)
                    else:
                        await self.executions.observe(link, act.id, guard=guard)
                except Exception as error:
                    # A saved native intent keeps its receipt/recovery owner;
                    # a generic failed reply could contradict a committed act.
                    logger.warning("Telegram Work action observation deferred: action=%s cause=%s", act.id, type(error).__name__)
            return True

        return await await_drained(perform())
