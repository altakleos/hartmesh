"""Optional Work callback dispatcher; installation belongs to the supervisor."""

import logging
from copy import deepcopy

from app.telegram_bots.authority import AccessOff, CeilingRefused
from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.errors import text_key_for
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_action_retries import WorkActionRetries
from app.telegram_bots.work_closed_inputs import WorkClosedInputs
from app.telegram_bots.work_control_admission import WorkControlAdmissions
from app.telegram_bots.work_followup_refresh import WorkRoutingFollowupRecovery
from app.telegram_bots.work_input_refresh import WorkInputRefresh
from app.telegram_bots.work_interaction_delivery import WorkInteractionPlans
from app.telegram_bots.work_refusal_recovery import WorkRefusalRecovery
from app.telegram_bots.work_request_recovery import WorkRequestRecovery
from app.telegram_bots.work_retained_drafts import WorkRetainedDrafts
from app.telegram_bots.work_review_recovery import WorkReviewRecovery
from app.telegram_bots.work_routing_admission import WorkRoutingAdmissions
from app.telegram_bots.work_routing_recovery import WorkRoutingRecovery
from app.telegram_bots.work_stale_recovery import WorkStaleRecovery
from deerflow.agent_instances.contract import AgentConflict, AgentDenied
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)
_VIEWS = {"hi_answer", "hi_cant_open", "hi_cannot", "hi_wrong", "decide_choice", "decide_own", "decide_note", "accept_open", "changes_open", "back"}
_ACTIONS = {"hi_choice": "respond", "decide_confirm": "decide", "accept_confirm": "accept", "changes_confirm": "changes_requested", "route_to": "route"}


class WorkCallbacks:
    def __init__(self, services, outcomes):
        self.services, self.outcomes, self.acts = services, outcomes, outcomes.acts
        self.interactions = WorkInteractionPlans(outcomes.plans)
        self.controls = WorkControlAdmissions(outcomes.plans, self.acts)
        self.closed = WorkClosedInputs(services, self.controls)
        self.refusals = WorkRefusalRecovery(self.controls)
        self.stale = WorkStaleRecovery(self.controls, self.interactions)
        self.recovery = WorkRequestRecovery(outcomes.plans)
        self.drafts = WorkRetainedDrafts(outcomes.plans)
        self.input_refresh = WorkInputRefresh(self.controls)
        self.review_recovery = WorkReviewRecovery(outcomes.plans, controls=self.controls)
        self.routing = WorkRoutingAdmissions(outcomes.routing, self.acts) if outcomes.routing is not None else None
        self.routing_recovery = WorkRoutingRecovery(outcomes.routing) if outcomes.routing is not None else None
        self.followups, self.retries = WorkRoutingFollowupRecovery(outcomes), WorkActionRetries(outcomes)

    async def execute_callback(self, channel, row, *, update_id=None, query_id=None):
        if await self.closed.execute_callback(channel, row, update_id=update_id):
            return True
        if row.kind not in _VIEWS | _ACTIONS.keys() | {"reveal", "act_check", "dismiss"}:
            return False

        async def perform():
            link = deepcopy(channel.link)
            async with self.outcomes.outbox.sessions() as reader:
                parent = await reader.get(TelegramOutboxRow, row.payload.get("anchor_outbox_id"))
                if parent is None or not ("work_request_basis" in parent.payload or "work_outcome_basis" in parent.payload):
                    return False
                if (row.link_id, row.binding_generation, row.chat_id) != (link.id, link.binding_generation, link.chat_id):
                    raise CallbackRefused()
                family = parent.payload.get("work_request_basis", {}).get("version")
            guard = self.services.commands._guard(channel, link)
            scope = {"chat_id": row.chat_id, "message_id": row.message_id, "message_thread_id": row.message_thread_id, "guard": guard}

            def current():
                return channel._callback_current(link)

            try:
                if family == 4 and row.kind in {"changes_open", "dismiss", "back", "decide_note"}:
                    await self.drafts.navigate(link, row.id, **scope)
                elif row.kind == "dismiss":
                    return False
                elif row.kind in _VIEWS:
                    await self.interactions.open(link, row.id, **scope)
                elif row.kind == "reveal":
                    recovery = self.routing_recovery if family in {5, 6} else self.recovery
                    if recovery is None:
                        raise CallbackRefused()
                    await recovery.reveal(link, row.id, **scope)
                elif row.kind == "act_check":
                    result = await self.retries.retry(link, row.id, **scope)
                    await self._observe(link, result.id, guard=guard)
                else:
                    admission = self.routing if row.kind == "route_to" else self.controls
                    if admission is None:
                        raise CallbackRefused()
                    try:
                        staged = await admission.stage(link, row.id, **scope)
                    except AgentDenied as error:
                        if row.kind == "route_to" and error.reason == "recipient-ineligible":
                            await self._refresh_routing(link, row, parent, scope)
                            return True
                        raise
                    # This is the saved operation, never another call to the
                    # action constructor. Unknown results remain journal-owned.
                    try:
                        if family == 4 and staged.state == "pending" and staged.reason == "attempt-unresolved":
                            try:
                                if await self.input_refresh.refresh(link, row.id, **scope) is not None:
                                    return True
                            except ProductSourceRetired:
                                # A known non-effect may still check its original
                                # immutable operation. Only a conclusive conflict
                                # can transfer its note to closed-review recovery.
                                pass
                        elif staged.kind in {"decide", "accept"} and staged.state == "pending" and staged.reason == "attempt-unresolved":
                            if await self.refusals.refresh(link, row.id, **scope) is not None:
                                return True
                        if staged.attempts == 0 or staged.state == "pending" and staged.reason in {"attempt-unresolved", "actions-paused"}:
                            expected = {"id": staged.id, "kind": staged.kind, "body": staged.body, "attempts": staged.attempts}
                            result = await self.acts.check(staged.id, expected=expected, attempt_current=current)
                        else:
                            result = await self.acts.reconcile_receipt(staged.id)
                    except Exception as error:
                        logger.warning("Work action observation deferred (%s)", type(error).__name__)
                        return True
                    if result.kind == "route" and result.state == "refused" and result.reason == "recipient-ineligible":
                        await self._refresh_routing(link, row, parent, scope)
                    elif result.state == "conflict" and row.kind != "route_to":
                        refresh = self.input_refresh if family == 4 else self.stale
                        if await refresh.refresh(link, row.id, **scope) is None:
                            await self._observe(link, result.id, guard=guard)
                    else:
                        await self._observe(link, result.id, guard=guard)
            except AgentConflict as error:
                if row.kind != "route_to" and error.reason != "attempt-unresolved":
                    try:
                        refresh = self.input_refresh if family == 4 else self.stale
                        if await refresh.refresh(link, row.id, **scope) is not None:
                            return True
                    except ProductSourceRetired:
                        raise CallbackRefused("stale.not_waiting") from None
                raise CallbackRefused(text_key_for(_ACTIONS.get(row.kind, "respond"), error)) from None
            except (AgentDenied, AccessOff, CeilingRefused) as error:
                raise CallbackRefused(text_key_for(_ACTIONS.get(row.kind, "respond"), error)) from None
            except ProductSourceRetired:
                if family == 4 and row.kind in {"changes_confirm", "changes_open"} and await self.review_recovery.from_callback(link, row.id, **scope):
                    return True
                raise CallbackRefused("stale.not_waiting") from None
            except CallbackRefused:
                raise
            except Exception as error:
                # A storage/read failure may follow an admitted operation.
                # Its receipt observer supplies the eventual result; a generic
                # 'failed' reply here could contradict the saved action.
                logger.warning("Work callback observation deferred (%s)", type(error).__name__)
            return True

        return await await_drained(perform())

    async def _refresh_routing(self, link, row, parent, scope):
        if "work_outcome_basis" in parent.payload:
            await self.followups.refresh(link, row.id, **scope)
        elif self.routing_recovery is not None:
            await self.routing_recovery.refresh_choice(link, row.id, **scope)
        else:
            raise CallbackRefused()

    async def _observe(self, link, act_id, *, guard):
        try:
            await self.outcomes.plan(link, act_id, guard=guard)
        except Exception as error:
            logger.warning("Work outcome observation deferred (%s)", type(error).__name__)
