"""Installed proposal owners use actual SQL, SDK controls and queue handoff.

Remote source documents and run admission are explicit protocol scaffolds.
"""

import asyncio
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from test_telegram_assignment_proposal_cards import db as db
from test_telegram_assignment_proposal_cards import proposal_cards as proposal_cards
from test_telegram_assignment_proposal_cards import work_delivery as work_delivery
from test_telegram_assignment_proposal_cards import work_pages as work_pages
from test_telegram_callback_commands import press
from test_telegram_work_chat_runtime import installed_work_runtime
from test_telegram_work_interaction_delivery import selected
from test_telegram_work_preview_recovery import dispatch

from app.channels.message_bus import MessageBus
from app.telegram_bots.publication import PublicationCoordinator
from app.telegram_bots.turns import ChatTurns
from deerflow.persistence.telegram_bots.model import TelegramActRow


@pytest.mark.asyncio
@pytest.mark.parametrize("permissions", [1, 3, 7])
async def test_installed_chat_adopts_proposal_and_just_answer_queues_only_the_choice(proposal_cards, monkeypatch, permissions, edge=None):
    from test_telegram_chat_turns import ControlledView

    monkeypatch.setattr("app.telegram_bots.streaming.LivePresentation", ControlledView)
    e = proposal_cards
    host = e.channel._services
    bus = MessageBus()
    coordinator = PublicationCoordinator(inbox=host.outbox.inbox, outbox=host.outbox, bus=bus, http_owner=e.proposal_http, capture_attempt=e.capture_proposal_attempt)
    host.publications, host.assignment_proposal_source = coordinator, e.proposal_source
    host.publication_authority = SimpleNamespace(capture=e.capture_proposal_attempt)
    owner = await installed_work_runtime(e)
    host.thread_client = e.proposal_http
    coordinator.host = host
    turns = ChatTurns(host)
    host.turns = turns
    release, preparing = asyncio.Event(), asyncio.Event()

    async def original_thread(*args, **kwargs):
        from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

        async with e.db.sessions.begin() as writer:
            grant = await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))
            grant.permissions = permissions
        return e.proposal_record.scope.thread_id

    async def no_run(*args, **kwargs):
        return None

    host.threads = SimpleNamespace(ensure_callback_thread=original_thread)
    turns._observer = SimpleNamespace(watch=no_run, read_run=no_run, retire=lambda record: None, drain=no_run)
    try:
        assert coordinator.assignment_proposal_planner is not None
        e.identity = await coordinator.assignment_proposal_planner(e.proposal_record, e.link, guard=e.proposal_guard)
        await dispatch(e, 7811)
        assert not e.dispatch_errors, e.dispatch_errors
        control = await selected(e, "assign_answer")
        if edge == "full":
            from _telegram_bot_test_support import make_update

            turns.queue.depth = 1

            async def hold_thread(*args, **kwargs):
                preparing.set()
                await release.wait()
                return e.proposal_record.scope.thread_id

            host.threads.ensure_thread = hold_thread
            assert await e.channel.handle_update(make_update(8801, account=e.link.chat_id, text="An independent current question"))
            await asyncio.wait_for(preparing.wait(), 5)
            current = next(iter(turns._turns.values()))
            assert await e.channel.handle_update(make_update(8802, account=e.link.chat_id, text="An independent queued question"))
            await e.channel._drain_ingress()
            assert turns.queue.waiting_count(e.link, None) == 1
        if edge == "observe_failure":

            async def failed_presentation(*args, guard=None, **kwargs):
                assert guard is not None
                guard.current()
                raise RuntimeError("Presentation unavailable after committed answer")

            monkeypatch.setattr(owner.chat.assignments, "observe", failed_presentation)
        elif edge == "retire_observation":
            observe = owner.chat.assignments.observe

            async def retire_presentation(*args, guard=None, **kwargs):
                e.active[0] = False
                return await observe(*args, guard=guard, **kwargs)

            monkeypatch.setattr(owner.chat.assignments, "observe", retire_presentation)
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 8811, control.id, message_id=7811))
        await e.channel._drain_ingress()
        if edge == "full":
            from app.telegram_bots.producer import DeliveryEligibility
            from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow

            async with e.db.sessions() as reader:
                outcome = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:submission-full:" + control.id))
                assert outcome is not None
                outcome_id = outcome.id
            eligibility = e.sender.eligibility

            async def only_retry_parent(link, item, part=None):
                return await eligibility(link, item, part) if item.id == outcome_id else DeliveryEligibility.wait()

            e.sender.eligibility = only_retry_parent
            e.identity = outcome_id
            e.clock.advance(10)
            await dispatch(e, 7812)
            e.sender.eligibility = eligibility
            assert not e.dispatch_errors, e.dispatch_errors
            async with e.db.sessions() as reader:
                retry = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == outcome_id, TelegramCallbackRow.kind == "retry_message"))
            assert await e.channel.handle_update(make_update(8803, account=e.link.chat_id, text="/stop"))
            release.set()
            await asyncio.wait_for(current.done.wait(), 5)
            e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
            assert await e.channel.handle_update(press(e, 8812, retry.id, message_id=7812))
            await e.channel._drain_ingress()
            e.retry_control = retry
        if edge == "retire_observation":
            from app.telegram_bots.inbox import CALLBACK_PUBLICATION_KEY
            from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

            with pytest.raises(asyncio.QueueEmpty):
                bus.get_inbound_nowait()
            async with e.db.sessions() as reader:
                saved = await reader.get(TelegramCallbackRow, control.id)
                assert saved.payload[CALLBACK_PUBLICATION_KEY]["state"] == "accepted"
                root = await reader.scalar(select(TelegramActRow).where(TelegramActRow.kind == "delegate"))
                assert root.state == "refused" and root.reason == "answered-here"
            return
        message = await asyncio.wait_for(bus.get_inbound(), 5)
        bus.inbound_task_done()
        assert message.text != e.proposal_input.sanitized_input["text"]
        assert message.metadata["human_message_kwargs"]["assignment_card_response"] == {
            "version": 1,
            "kind": "assignment_card_response",
            "tool_call_id": e.proposal_basis["messages"][-1]["content"]["tool_call_id"],
            "choice": "answer_here",
        }
        assert message.metadata["human_message_kwargs"]["shows_cards"] == (["assignment_card"] if permissions & 3 == 3 else [])
        assert message.execution_context.scope.thread_id == e.proposal_record.scope.thread_id
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 8811, control.id, message_id=7811))
        await e.channel._drain_ingress()
        with pytest.raises(asyncio.QueueEmpty):
            bus.get_inbound_nowait()
        async with e.db.sessions() as reader:
            root = await reader.scalar(select(TelegramActRow).where(TelegramActRow.kind == "delegate"))
            assert (root.state, root.reason) == ("refused", "answered-here")
            assert not root.operation_ids
    finally:
        release.set()
        await turns.stop()
        await owner.stop()
        await coordinator.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["live", "cold"])
async def test_live_observer_adopts_only_verified_proposal_with_installed_owner(proposal_cards, mode):
    from app.telegram_bots.progress import RunObserver

    e = proposal_cards
    host = e.channel._services
    coordinator = PublicationCoordinator(inbox=host.outbox.inbox, outbox=host.outbox, bus=MessageBus(), http_owner=e.proposal_http, capture_attempt=e.capture_proposal_attempt)
    host.publications, host.assignment_proposal_source = coordinator, e.proposal_source
    owner = await installed_work_runtime(e)
    observer = RunObserver(coordinator=coordinator, http_owner=e.proposal_http, capture_attempt=e.capture_proposal_attempt, is_current=lambda record: e.source_active)
    try:

        async def adopt():
            if mode == "cold":
                result = await coordinator.observe(e.proposal_record, e.link, max_pages=8)
                assert result.state == "planned"
                return result.outbox_id
            return await observer.plan_final(e.proposal_record, e.link)

        e.identity = await adopt()
        assert e.identity is not None
        assert await adopt() == e.identity
        await dispatch(e, 7821)
        assert not e.dispatch_errors, e.dispatch_errors
        async with e.db.sessions() as reader:
            roots = list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "delegate")))
            assert len(roots) == 1 and roots[0].state == "draft" and not roots[0].operation_ids
        assert all(request.method == "GET" for request in e.source_reads)
    finally:
        await observer.drain()
        await owner.stop()
        await coordinator.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("edge", ["observe_failure", "retire_observation"])
async def test_committed_just_answer_handoff_owns_presentation_failure_and_retirement(proposal_cards, monkeypatch, edge):
    await test_installed_chat_adopts_proposal_and_just_answer_queues_only_the_choice(proposal_cards, monkeypatch, 7, edge=edge)


@pytest.mark.asyncio
async def test_full_queue_retains_just_answer_for_actual_notice_retry(proposal_cards, monkeypatch):
    await test_installed_chat_adopts_proposal_and_just_answer_queues_only_the_choice(proposal_cards, monkeypatch, 7, edge="full")


@pytest.mark.asyncio
async def test_cancelling_live_adoption_drains_its_post_commit_codec(proposal_cards, monkeypatch):
    import threading

    from app.telegram_bots import model_source
    from app.telegram_bots.progress import RunObserver

    e = proposal_cards
    host = e.channel._services
    coordinator = PublicationCoordinator(inbox=host.outbox.inbox, outbox=host.outbox, bus=MessageBus(), http_owner=e.proposal_http, capture_attempt=e.capture_proposal_attempt)
    host.publications, host.assignment_proposal_source = coordinator, e.proposal_source
    owner = await installed_work_runtime(e)
    observer = RunObserver(coordinator=coordinator, http_owner=e.proposal_http, capture_attempt=e.capture_proposal_attempt, is_current=lambda record: e.source_active)
    armed, entered, release, finished = (threading.Event() for _ in range(4))
    planner, codec = coordinator.assignment_proposal_planner, model_source.validate_model_reply_basis

    async def adopted(*args, **kwargs):
        result = await planner(*args, **kwargs)
        armed.set()
        return result

    def held_codec(*args, **kwargs):
        result = codec(*args, **kwargs)
        if armed.is_set():
            entered.set()
            release.wait()
            finished.set()
        return result

    coordinator.assignment_proposal_planner = adopted
    monkeypatch.setattr(model_source, "validate_model_reply_basis", held_codec)
    task = asyncio.create_task(observer.plan_final(e.proposal_record, e.link))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and not finished.is_set()
        e.source_active = False
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set()
        async with e.db.sessions() as reader:
            roots = list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "delegate")))
            assert len(roots) == 1 and roots[0].state == "draft"
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await observer.drain()
        await owner.stop()
        await coordinator.stop()
