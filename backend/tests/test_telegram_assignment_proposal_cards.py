"""Real SQL/card custody with scripted remote output and retained HTTP ownership."""

from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
from _telegram_bot_test_support import ManualClock, make_update
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_assignment_proposal_source import proposal_basis
from test_telegram_publication_observation import ACK
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.acts import Acts
from app.telegram_bots.assignment_delivery import AssignmentPlans
from app.telegram_bots.assignment_proposal_source import AssignmentProposalSource
from app.telegram_bots.assignment_sources import AssignmentReader
from app.telegram_bots.gateway_client import GatewayClientOwner
from app.telegram_bots.inbox import publication_identity
from app.telegram_bots.work_news_delivery import WorkNewsSources
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow, TelegramOutboxRow


@pytest_asyncio.fixture
async def proposal_cards(work_delivery, request):
    e = work_delivery
    services = e.channel._services
    e.clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = e.clock.now
    e.channel._now = e.clock.now
    services.callbacks.clock = e.clock.now
    e.acts = Acts(e.db.sessions, settings=e.db.settings, callbacks=services.callbacks, outbox=e.sender.outbox, work=e.human.work, authority=e.live.authority, links=e.db.links)
    e.assignments = AssignmentPlans(reader=AssignmentReader(e.acts), outbox=e.sender.outbox, callbacks=services.callbacks)
    services.assignment_sources = WorkNewsSources(e.assignments)
    e.source_reads, e.source_active = [], True

    async def respond(request):
        e.source_reads.append(request)
        if request.url.path.endswith("/threads/proposal-original-thread"):
            return httpx.Response(200, headers=ACK, json={"thread_id": "proposal-original-thread", "owner_user_id": e.link.owner_user_id, "agent_instance": {"id": e.link.agent_ref}, "metadata": {}})
        if request.url.path.endswith("/output"):
            return httpx.Response(200, headers=ACK, json=e.proposal_basis["projection"])
        return httpx.Response(200, headers=ACK, json={"data": e.proposal_basis["messages"], "has_more": False})

    e.proposal_http = GatewayClientOwner(gateway_url="https://gateway.example", langgraph_url="https://runtime.example/api", transport=httpx.MockTransport(respond))
    inbox = e.sender.outbox.inbox
    mark, _ = await inbox.accept(e.link, make_update(7301, account=e.link.chat_id, text="Review the guide"), sanitized_input={"type": "text", "text": "Review the guide"})
    e.proposal_input = await inbox.route(mark, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[])
    e.proposal_inputs = [e.proposal_input]
    if getattr(request, "param", {}).get("grouped"):
        second, _ = await inbox.accept(e.link, make_update(7302, account=e.link.chat_id, text="Include the appendix"), sanitized_input={"type": "text", "text": "Include the appendix"})
        e.proposal_inputs.append(await inbox.route(second, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[]))
    original_guard = await guard_for(e)

    class PublicationAttemptGuard:
        captured_link = original_guard.captured_link

        def current(self):
            original_guard.current()
            if e.source_active is not True:
                raise ValueError("retired-original-source")
            return True

        async def check(self, writer):
            value = await original_guard.check(writer)
            self.current()
            return value

    e.proposal_guard = PublicationAttemptGuard()
    original = await inbox.reserve_publication(e.link, e.proposal_inputs, thread_id="proposal-original-thread", endpoint_id=e.proposal_http.endpoint_id, guard=e.proposal_guard)
    assert await inbox.mark_dispatch_started(original, guard=e.proposal_guard)
    e.proposal_record = await inbox.bind_publication_run(original, run_id="proposal-original-run", guard=e.proposal_guard)
    _seed_record, e.proposal_basis, _draft = proposal_basis()
    scope = e.proposal_record.scope
    e.proposal_basis["publication"] = publication_identity(e.proposal_record)
    e.proposal_basis["messages"][0]["content"]["additional_kwargs"]["channel_turn_id"] = e.proposal_record.publication_id
    e.proposal_basis["messages"][0]["content"]["content"] = "\n\n".join(value.sanitized_input["text"] for value in e.proposal_inputs)
    output, inventory = e.proposal_basis["projection"]["output"], e.proposal_basis["projection"]["inventory"]
    for value in (output, output["output_evidence"], inventory["document"]):
        value.update(owner_user_id=scope.owner_user_id, thread_id=scope.thread_id, run_id=e.proposal_record.run_id)
    output["channel_execution"].update(publication_id=e.proposal_record.publication_id, owner_user_id=scope.owner_user_id, thread_id=scope.thread_id)
    for row in e.proposal_basis["messages"]:
        row.update(user_id=scope.owner_user_id, thread_id=scope.thread_id, run_id=e.proposal_record.run_id)
    from test_telegram_assignment_proposal_source import reseal

    reseal(e.proposal_basis)

    # The remote read admission is a controlled seam; creation and every card
    # SDK fence retain the actual current account, grant and SQL source checks.
    class Admission:
        def current(self, operation):
            return e.source_active

        async def authorize(self, operation):
            if not e.source_active:
                raise ValueError("retired-original-source")

    async def capture(link, *, thread_id, endpoint_id):
        assert (link.id, thread_id, endpoint_id) == (e.link.id, scope.thread_id, e.proposal_http.endpoint_id)
        return SimpleNamespace(admission=Admission(), read_guard=await guard_for(e), create_guard=await guard_for(e), thread_id=thread_id, endpoint_id=endpoint_id)

    async def read_output(record, link):
        from app.gateway.run_output import RunOutputProjectionV2

        assert record == e.proposal_record and link.id == e.link.id
        return RunOutputProjectionV2.model_validate(e.proposal_basis["projection"])

    e.proposal_source = AssignmentProposalSource(SimpleNamespace(read_output=read_output), e.proposal_http, capture)
    e.capture_proposal_attempt = capture
    from app.telegram_bots.model_source import ModelSource

    services.model_source = ModelSource(read_output=read_output, assignment_proposal_source=e.proposal_source)
    from app.telegram_bots.assignment_proposal_plans import AssignmentProposalPlans

    e.proposals = AssignmentProposalPlans(e.assignments, source=e.proposal_source)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
    try:
        yield e
    finally:
        await e.proposal_http.stop()


@pytest.mark.asyncio
async def test_proposal_adopts_one_private_draft_and_original_primary(proposal_cards):
    e = proposal_cards
    e.identity = await e.proposals.plan_terminal(e.proposal_record, e.link, guard=e.proposal_guard)
    assert await e.proposals.plan_terminal(e.proposal_record, e.link, guard=e.proposal_guard) == e.identity
    async with e.db.sessions() as reader:
        roots = list(await reader.scalars(select(TelegramActRow)))
        assert len(roots) == 1 and roots[0].body["source"] == "tool" and roots[0].state == "draft"
        assert roots[0].body["draft"]["objective"] == "Review the guide" and not roots[0].operation_ids
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 1
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.dedupe_key == "reply:" + e.proposal_record.turn_id
        assert row.payload["reply_basis"]["version"] == 9
        assert row.payload["assignment_intent"]["draft_id"] == roots[0].id
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, e.proposal_input.message_id, e.link.chat_id))
        assert mark.outbox_id == e.identity and mark.route_complete and mark.state == "reply_pending"
        assert not any(part.get("message_ids") for part in row.parts)
        assert "Assignment proposed by" in row.parts[0]["visible_text"]
    assert await selected(e, "assign_answer") is not None


@pytest.mark.asyncio
async def test_existing_primary_with_missing_root_cannot_recreate_a_draft(proposal_cards):
    from app.telegram_bots.producer import ProductSourceRetired

    e = proposal_cards
    e.identity = await e.proposals.plan_terminal(e.proposal_record, e.link, guard=e.proposal_guard)
    async with e.db.sessions.begin() as writer:
        root = await writer.scalar(select(TelegramActRow))
        await writer.delete(root)
    with pytest.raises((ValueError, ProductSourceRetired)):
        await e.proposals.plan_terminal(e.proposal_record, e.link, guard=e.proposal_guard)
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(TelegramActRow)) == 0


@pytest.mark.asyncio
async def test_actual_card_ack_owns_controls_and_finishes_original_delivery(proposal_cards):
    e = proposal_cards
    e.proposal_basis["messages"][1]["content"]["content"] = "Manager wording that must not be sent"
    e.identity = await e.proposals.plan_terminal(e.proposal_record, e.link, guard=e.proposal_guard)
    await dispatch(e, 7401)
    assert not e.dispatch_errors, e.dispatch_errors
    button = await selected(e, "assign_answer")
    assert button.message_id == 7401 and button.payload["presentation_pending"] is False
    calls = [call.parameters for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 1 and "Review the guide" in calls[0]["text"]
    assert "Manager wording" not in calls[0]["text"]
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, e.proposal_input.message_id, e.link.chat_id))
        assert mark.state == "done" and mark.outbox_id == e.identity
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 1


@pytest.mark.asyncio
async def test_proposal_full_ack_save_keeps_the_same_primary_and_one_command(proposal_cards):
    from app.telegram_bots.assignment_controls import AssignmentAdmissions
    from app.telegram_bots.callbacks import CallbackRefused

    e = proposal_cards
    e.identity = await e.proposals.plan_terminal(e.proposal_record, e.link, guard=e.proposal_guard)
    button = await selected(e, "assign_save")
    admission = AssignmentAdmissions(e.assignments, e.acts)
    with pytest.raises(CallbackRefused):
        await admission.stage(e.link, button.id, chat_id=e.link.chat_id, message_id=7402, message_thread_id=None, guard=e.proposal_guard)
    await dispatch(e, 7402)
    assert not e.dispatch_errors, e.dispatch_errors
    button = await selected(e, "assign_save")
    act = await admission.stage(e.link, button.id, chat_id=e.link.chat_id, message_id=button.message_id, message_thread_id=None, guard=e.proposal_guard)
    assert (await e.acts.check(act.id)).state == "done"
    assert await e.assignments.observe(e.link, act.continuation["delegate_source"]["draft_id"]) == e.identity
    assert await e.proposals.plan_terminal(e.proposal_record, e.link, guard=e.proposal_guard) == e.identity
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 2
        assert len(list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_intent"]["draft_id"].as_string() == act.continuation["delegate_source"]["draft_id"])))) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["input", "inspect", "source", "draft"])
async def test_final_proposal_sdk_fence_rechecks_original_input_and_source(proposal_cards, monkeypatch, damage):
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = proposal_cards
    e.identity = await e.proposals.plan_terminal(e.proposal_record, e.link, guard=e.proposal_guard)
    sources = e.channel._services.assignment_sources
    get = sources.settings.get
    changed = []

    async def retire(*args, **kwargs):
        result = await get(*args, **kwargs)
        if not changed:
            changed.append(True)
            if damage == "source":
                e.source_active = False
            else:
                async with e.db.sessions.begin() as writer:
                    if damage == "input":
                        row = await writer.get(TelegramInboundMarkRow, (e.link.id, e.proposal_input.message_id, e.link.chat_id))
                        row.sanitized_input = {"type": "text", "text": "Changed input"}
                    elif damage == "draft":
                        from copy import deepcopy

                        row = await writer.get(TelegramActRow, (await selected(e, "assign_answer")).target_id)
                        body = deepcopy(row.body)
                        body["draft"]["objective"] = "Different assignment absent from the original proposal"
                        row.body = body
                    else:
                        (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 1
        return result

    monkeypatch.setattr(sources.settings, "get", retire)
    await dispatch(e, 7403)
    assert changed and not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]


@pytest.mark.asyncio
async def test_just_answer_freezes_one_new_message_in_the_original_conversation(proposal_cards):
    from app.telegram_bots.assignment_answers import AssignmentAnswers
    from app.telegram_bots.render import render_plain

    e = proposal_cards
    e.identity = await e.proposals.plan_terminal(e.proposal_record, e.link, guard=e.proposal_guard)
    await dispatch(e, 7410)
    assert not e.dispatch_errors, e.dispatch_errors
    button = await selected(e, "assign_answer")
    answers = AssignmentAnswers(e.assignments)
    stage = await answers.stage(e.link, button.id, update_id=8410, chat_id=e.link.chat_id, message_id=7410, message_thread_id=None, guard=e.proposal_guard)
    replay = await answers.stage(e.link, button.id, update_id=8410, chat_id=e.link.chat_id, message_id=7410, message_thread_id=None, guard=e.proposal_guard)
    assert stage == replay
    assert stage.text == render_plain("assign.just_answer_message", "en-US")
    assert stage.text != "Review the guide" and stage.thread_id == e.proposal_record.scope.thread_id
    assert stage.human_message_kwargs == {"assignment_card_response": {"version": 1, "kind": "assignment_card_response", "tool_call_id": "proposal-call", "choice": "answer_here"}}
    assert stage.callback_id == button.id and stage.record is None
    async with e.db.sessions() as reader:
        root = await reader.get(TelegramActRow, button.target_id)
        assert (root.state, root.reason, root.attempts, root.operation_ids) == ("refused", "answered-here", 0, [])
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 1
    assert await e.sender.outbox.inbox.read_submission(e.link, stage, guard=e.proposal_guard) == stage


@pytest.mark.asyncio
async def test_just_answer_cannot_use_unacknowledged_or_already_saved_controls(proposal_cards):
    from app.telegram_bots.assignment_answers import AssignmentAnswers
    from app.telegram_bots.callbacks import CallbackRefused

    e = proposal_cards
    e.identity = await e.proposals.plan_terminal(e.proposal_record, e.link, guard=e.proposal_guard)
    button = await selected(e, "assign_answer")
    with pytest.raises((CallbackRefused, ValueError)):
        await AssignmentAnswers(e.assignments).stage(e.link, button.id, update_id=8411, chat_id=e.link.chat_id, message_id=7411, message_thread_id=None, guard=e.proposal_guard)
    async with e.db.sessions() as reader:
        root = await reader.get(TelegramActRow, button.target_id)
        assert root.state == "draft" and not root.operation_ids


@pytest.mark.asyncio
async def test_just_answer_dispatches_the_saved_fixed_message_once(proposal_cards):
    from app.channels.message_bus import MessageBus
    from app.telegram_bots.assignment_answers import AssignmentAnswers
    from app.telegram_bots.publication import PublicationCoordinator

    e = proposal_cards
    e.identity = await e.proposals.plan_terminal(e.proposal_record, e.link, guard=e.proposal_guard)
    await dispatch(e, 7412)
    button = await selected(e, "assign_answer")
    stage = await AssignmentAnswers(e.assignments).stage(e.link, button.id, update_id=8412, chat_id=e.link.chat_id, message_id=7412, message_thread_id=None, guard=e.proposal_guard)
    inbox = e.sender.outbox.inbox
    with pytest.raises(ValueError):
        await inbox.reserve_callback_publication(e.link, button.id, thread_id="a-new-conversation", endpoint_id=e.proposal_http.endpoint_id, guard=e.proposal_guard)
    record = await inbox.reserve_callback_publication(e.link, button.id, thread_id=stage.thread_id, endpoint_id=e.proposal_http.endpoint_id, guard=e.proposal_guard)
    bus = MessageBus(inbound_queue_maxsize=4)
    coordinator = PublicationCoordinator(inbox=inbox, outbox=e.sender.outbox, bus=bus, http_owner=e.proposal_http, capture_attempt=e.capture_proposal_attempt)
    try:
        admitted = await coordinator.dispatch_record(record, e.link)
        assert admitted.dispatch_started and admitted.owner_callback_id == button.id
        message = bus.get_inbound_nowait()
        bus.inbound_task_done()
        assert message.text == stage.text and message.text != "Review the guide"
        assert message.files == [] and message.execution_context.scope.thread_id == stage.thread_id
        assert message.metadata["human_message_kwargs"]["assignment_card_response"] == stage.human_message_kwargs["assignment_card_response"]
        assert await coordinator.dispatch_record(record, e.link) == admitted
        assert bus.inbound_queue.empty()
    finally:
        await coordinator.stop()
        bus.discard_pending_inbound()


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["observe", "replay"])
async def test_changed_live_draft_body_cannot_replace_qualified_proposal(proposal_cards, entry):
    from copy import deepcopy

    from app.telegram_bots.producer import ProductSourceRetired

    e = proposal_cards
    e.identity = await e.proposals.plan_terminal(e.proposal_record, e.link, guard=e.proposal_guard)
    button = await selected(e, "assign_answer")
    assert await e.assignments.observe(e.link, button.target_id) == e.identity
    async with e.db.sessions.begin() as writer:
        root = await writer.get(TelegramActRow, button.target_id)
        body = deepcopy(root.body)
        body["draft"]["objective"] = "Different assignment absent from the original proposal"
        root.body = body
    with pytest.raises((ValueError, ProductSourceRetired)):
        if entry == "observe":
            await e.assignments.observe(e.link, button.target_id)
        else:
            await e.proposals.plan_terminal(e.proposal_record, e.link, guard=e.proposal_guard)
