"""Network audit records publication intent, never directly applies a grant."""

import importlib

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_callbacks import callbacks_environment as callbacks_environment
from test_telegram_publication_authority import attempt_environment as attempt_environment
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment
from test_telegram_publication_custody import publication_environment as publication_environment
from test_telegram_submission_publication import submission_environment as submission_environment

from deerflow.persistence.telegram_bots.model import TelegramActRow


@pytest.mark.asyncio
async def test_network_audit_captures_actual_card_body_in_borrowed_writer(callbacks_environment):
    e = callbacks_environment
    body = {"thread_id": e.thread_id, "request_id": "network-request", "option_id": "allow_temporary", "host": "api.example.com", "port": 8443}
    plan = await e.plan(keyboard=[[{"label": "label.allow_site", "kind": "network", "target_kind": "thread", "target_id": e.thread_id, "payload": body}]], message_id=902)
    identity = plan.group.callback_ids[0]
    acts = importlib.import_module("app.telegram_bots.acts").Acts(e.sessions, settings=e.db.settings, callbacks=e.callbacks, outbox=e.outbox)
    async with e.callbacks.writer(e.link, e.guard_factory()) as writer:
        saved = await acts.write_network(e.link, callback_id=identity, body=body, guard=e.guard_factory(), session=writer)
        assert saved.state == "pending" and saved.attempts == 0 and saved.first_attempt_at is None
        assert dict(saved.body) == body and saved.owner_user_id == e.link.owner_user_id
        assert saved.operation_ids == ()
    async with e.sessions() as reader:
        actual = await reader.get(TelegramActRow, identity)
        assert actual.state == "pending" and actual.result is None
    with pytest.raises(ValueError):
        async with e.callbacks.writer(e.link, e.guard_factory()) as writer:
            await acts.write_network(e.link, callback_id=identity, body={**body, "host": "changed.example.com"}, guard=e.guard_factory(), session=writer)


@pytest.mark.asyncio
async def test_failed_required_publication_rolls_back_network_audit(callbacks_environment):
    e = callbacks_environment
    body = {"thread_id": e.thread_id, "request_id": "network-request", "option_id": "deny", "host": "api.example.com", "port": 443}
    plan = await e.plan(keyboard=[[{"label": "label.keep_blocked", "kind": "network", "target_kind": "thread", "target_id": e.thread_id, "payload": body}]], message_id=902)
    identity = plan.group.callback_ids[0]
    acts = importlib.import_module("app.telegram_bots.acts").Acts(e.sessions, settings=e.db.settings, callbacks=e.callbacks, outbox=e.outbox)
    with pytest.raises(RuntimeError, match="publication failed"):
        async with e.callbacks.writer(e.link, e.guard_factory()) as writer:
            await acts.write_network(e.link, callback_id=identity, body=body, guard=e.guard_factory(), session=writer)
            raise RuntimeError("publication failed")
    async with e.sessions() as reader:
        assert await reader.get(TelegramActRow, identity) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("submission_environment", ["network"], indirect=True)
async def test_network_audit_completes_only_after_same_publication_actual_enqueue(submission_environment):
    e = submission_environment
    acts = importlib.import_module("app.telegram_bots.acts").Acts(e.db.sessions, settings=e.db.settings, callbacks=e.callbacks, outbox=e.outbox)

    async def confirmed(record, writer):
        await acts.confirm_network_publication(record, session=writer)

    e.inbox.on_callback_confirmed = confirmed
    async with e.callbacks.writer(e.link, e.guard) as writer:
        await acts.write_network(e.link, callback_id=e.callback_id, body=e.network_body, guard=e.guard, session=writer)
        stage = await e.stage(session=writer)
    assert stage.callback_id == e.callback_id and e.bus.inbound_queue.empty()
    record = await e.inbox.reserve_callback_publication(e.link, e.callback_id, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id, guard=e.guard)
    async with e.callbacks.writer(e.link, e.guard) as writer:
        with pytest.raises(ValueError):
            await acts.confirm_network_publication(record, session=writer)
    async with e.db.sessions() as reader:
        saved = await reader.get(TelegramActRow, e.callback_id)
        assert saved.state == "pending" and saved.result is None
    dispatched = await e.coordinator.dispatch_record(record, e.link)
    sent = e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    assert sent.metadata["human_message_kwargs"]["human_input_response"] == e.response
    assert sent.text == e.response["value"] and sent.execution_context.thread_id == e.thread_id
    async with e.db.sessions() as reader:
        saved = await reader.get(TelegramActRow, e.callback_id)
        assert saved.state == "done" and saved.attempts == 1
        assert saved.result == {"turn_id": dispatched.turn_id, "publication_id": dispatched.publication_id}
        assert saved.first_attempt_at is not None
    async with e.callbacks.writer(e.link, e.guard) as writer:
        replay = await acts.write_network(e.link, callback_id=e.callback_id, body=e.network_body, guard=e.guard, session=writer)
        assert replay.state == "done" and replay.attempts == 1
    await e.coordinator.dispatch_record(record, e.link)
    assert e.bus.inbound_queue.empty()


@pytest.mark.asyncio
@pytest.mark.parametrize("submission_environment", ["network_mismatch"], indirect=True)
async def test_network_source_mismatch_rolls_back_the_staged_audit(submission_environment):
    e = submission_environment
    acts = importlib.import_module("app.telegram_bots.acts").Acts(e.db.sessions, settings=e.db.settings, callbacks=e.callbacks, outbox=e.outbox)
    with pytest.raises(ValueError):
        async with e.callbacks.writer(e.link, e.guard) as writer:
            # The saved descriptor is internally consistent but names another
            # host than the exact source request. The whole intent must refuse.
            await acts.write_network(e.link, callback_id=e.callback_id, body=e.network_body, guard=e.guard, session=writer)
            await e.stage(session=writer)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, e.callback_id) is None
    assert e.bus.inbound_queue.empty()
    assert (await e.callbacks.get(e.callback_id)).consumed_at is None
