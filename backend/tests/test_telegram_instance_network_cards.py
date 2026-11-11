"""Instance cards use real grants/SQL/SDK; output protocol seeds are explicit."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from _telegram_human_input_test_support import request, source_basis
from _telegram_repository_test_support import db as db
from fastapi.responses import JSONResponse
from sqlalchemy import select, update
from test_telegram_bot_handlers import sent
from test_telegram_callback_commands import drain
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_file_exports import exports as exports
from test_telegram_instance_publication import publication as publication
from test_telegram_publication_observation import ACK, run_row

from app.telegram_bots.acts import Acts
from app.telegram_bots.callbacks import CallbackRefused, CallbackRepository, Prompts
from app.telegram_bots.forms import Forms
from app.telegram_bots.human_input import HumanInputs
from app.telegram_bots.human_input_source import HumanInputSource
from app.telegram_bots.model_source import ModelSource
from app.telegram_bots.producer import OutboxSender
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow

pytestmark = pytest.mark.asyncio


def network_request():
    return request(
        source="sandbox_network",
        input_mode="single_choice",
        details={"host": "example.test", "port": 443, "ttl_seconds": 300},
        options=[{"id": "allow_temporary", "label": "Allow", "value": "allow_temporary"}, {"id": "deny", "label": "Deny", "value": "deny"}],
    )


async def permissions(e, value):
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceGrantRow).where(AgentInstanceGrantRow.instance_id == e.link.agent_ref, AgentInstanceGrantRow.user_id == e.link.owner_user_id).values(permissions=value))


@pytest_asyncio.fixture
async def network(publication):
    e = publication
    e.services = e.channel._services
    e.clock = e.services.outbox.clock.__self__
    e.request = e.channel._requests[0]
    e.services.authority = e.live.authority
    e.services.prepare_live_owner = e.live.authority.prepare_live_owner
    e.services.protect_notice = e.live.authority.protect_notice
    e.services.eligibility = e.live.authority.delivery
    e.services.callbacks = CallbackRepository(e.db.sessions, config=e.services.config)
    e.services.prompts = Prompts(e.services.callbacks)
    e.services.forms = Forms(e.services.callbacks)
    e.services.acts = Acts(e.db.sessions, settings=e.db.settings, callbacks=e.services.callbacks, outbox=e.services.outbox)
    e.services.thread_client, e.services.publications, e.services.publication_authority = e.http_client, e.coordinator, e.host
    e.services.turns = SimpleNamespace(accept_staged=AsyncMock(return_value=True), retire=AsyncMock(), message_sent=lambda *args, **kwargs: None)
    reader = HumanInputSource(coordinator=e.coordinator, http_owner=e.http_client, capture_attempt=e.host.capture)
    e.services.model_source = ModelSource(read_output=e.coordinator.read_output, human_input_source=reader)
    e.services.human_inputs = HumanInputs(e.services, source=reader)
    saved = await e.coordinator.dispatch(e.link, [e.origin], thread_id=e.thread_id)
    guard = (await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http_client.endpoint_id)).read_guard
    e.record = await e.inbox.bind_publication_run(saved, run_id="coordinator-run", guard=guard)
    e.seed = source_basis(e.record, payload=network_request())

    @e.http_app.get("/api/threads/{thread_id}/runs/{run_id}/output")
    async def output(thread_id, run_id):
        assert (thread_id, run_id) == (e.thread_id, e.record.run_id)
        return JSONResponse(e.seed["projection"], headers=ACK)

    @e.http_app.get("/api/threads/{thread_id}/runs/{run_id}/messages")
    async def messages(thread_id, run_id):
        assert (thread_id, run_id) == (e.thread_id, e.record.run_id)
        return JSONResponse({"data": e.seed["messages"], "has_more": False}, headers=ACK)

    @e.http_app.get("/api/threads/{thread_id}/runs/page")
    async def page(thread_id):
        assert thread_id == e.thread_id
        return JSONResponse({"data": [{**run_row(e.record), "metadata": {"channel_turn_id": e.record.publication_id}, "status": "success"}], "has_more": False}, headers=ACK)

    @e.http_app.get("/api/threads/{thread_id}/runs/{run_id}")
    async def run(thread_id, run_id):
        assert (thread_id, run_id) == (e.thread_id, e.record.run_id)
        return JSONResponse({**run_row(e.record), "status": "success"}, headers=ACK)

    async with e.db.sessions.begin() as writer:
        for row in await writer.scalars(select(TelegramOutboxRow)):
            row.state, row.skip_reason = "skipped", "superseded"
    e.sender = OutboxSender(e.services.outbox, run_id="instance-network-cards", deliver_part=lambda **kw: e.channel.send_part(**{key: value for key, value in kw.items() if key != "link_id"}), eligibility=e.services.eligibility)
    try:
        yield e
    finally:
        await e.sender.stop()


async def offer(e):
    identity = await e.services.human_inputs.plan_terminal(e.record, e.link)
    assert identity
    return await e.services.outbox.get(e.link, identity)


async def test_manager_gets_only_the_actual_request_card(network):
    e = network
    await permissions(e, 7)
    await offer(e)
    e.request.enqueue("sendMessage", result=sent(e, identity=701))
    await drain(e)
    call = next(call for call in e.request.calls if call.endpoint == "sendMessage")
    assert "example.test" in call.parameters["text"]
    assert [row[0]["text"] for row in call.parameters["reply_markup"]["inline_keyboard"]] == ["Allow for 5 minutes", "Keep blocked"]


async def test_use_without_manage_cannot_publish_a_network_card(network):
    e = network
    with pytest.raises(CallbackRefused, match="net.blocked_ask"):
        await offer(e)
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "network")))
        assert not list(await reader.scalars(select(TelegramActRow)))


async def test_manager_withdrawal_holds_an_unsent_card(network):
    e = network
    await permissions(e, 7)
    item = await offer(e)
    await permissions(e, 1)
    assert (await e.services.eligibility(e.link, item, item.parts[-1])).decision != "allow"
    await drain(e)
    assert not any(call.endpoint == "sendMessage" for call in e.request.calls)


async def test_withdrawn_manager_press_writes_no_act_and_stages_no_answer(network):
    e = network
    await permissions(e, 7)
    await offer(e)
    e.request.enqueue("sendMessage", result=sent(e, identity=701))
    await drain(e)
    async with e.db.sessions() as reader:
        raw = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "network"))
        identity = raw.id
    row = await e.services.callbacks.get(identity)
    await permissions(e, 1)
    with pytest.raises(CallbackRefused, match="net.blocked_ask"):
        await e.services.human_inputs.execute_callback(e.channel, row, update_id=1500)
    e.services.turns.accept_staged.assert_not_awaited()
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))
        assert (await reader.get(TelegramCallbackRow, identity)).consumed_at is None
    e.request.enqueue("sendMessage", result=sent(e, identity=702))
    await e.channel._tell_press_failed(row, CallbackRefused("net.blocked_ask"), link=e.link, update_id=1500)
    await drain(e)
    assert "could not reach a site" in e.request.calls[-1].parameters["text"]


async def test_manager_withdrawal_inside_sdk_limiter_prevents_raw_card(network):
    e = network
    await permissions(e, 7)
    await offer(e)
    limiter, changed = e.channel.bot.rate_limiter, []

    async def waited(delay):
        limiter._short_hold_until = 0
        await permissions(e, 1)
        changed.append(True)

    limiter._sleep = waited
    limiter._short_hold_until = limiter._time() + 10
    e.request.enqueue("sendMessage", result=sent(e, identity=701))
    await drain(e)
    assert changed == [True]
    assert not any(call.endpoint == "sendMessage" for call in e.request.calls)


async def test_manager_withdrawal_inside_action_writer_rolls_back_the_press(network, monkeypatch):
    e = network
    await permissions(e, 7)
    await offer(e)
    e.request.enqueue("sendMessage", result=sent(e, identity=701))
    await drain(e)
    async with e.db.sessions() as reader:
        identity = (await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "network"))).id
    row = await e.services.callbacks.get(identity)
    actual, changed = e.services.callbacks.lock_current, []

    async def lock(*args, **kwargs):
        result = await actual(*args, **kwargs)
        if not changed:
            await kwargs["session"].execute(update(AgentInstanceGrantRow).where(AgentInstanceGrantRow.instance_id == e.link.agent_ref).values(permissions=1))
            changed.append(True)
        return result

    monkeypatch.setattr(e.services.callbacks, "lock_current", lock)
    with pytest.raises(CallbackRefused, match="net.blocked_ask"):
        await e.services.human_inputs.execute_callback(e.channel, row, update_id=1501)
    assert changed == [True]
    e.services.turns.accept_staged.assert_not_awaited()
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))
        assert (await reader.get(TelegramCallbackRow, identity)).consumed_at is None


async def test_current_manager_answer_keeps_existing_act_and_resume_protocol(network):
    e = network
    await permissions(e, 7)
    await offer(e)
    e.request.enqueue("sendMessage", result=sent(e, identity=701))
    await drain(e)
    async with e.db.sessions() as reader:
        rows = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "network")))
        identity = next(row.id for row in rows if row.payload["option_id"] == "allow_temporary")
    row = await e.services.callbacks.get(identity)
    await e.services.human_inputs.execute_callback(e.channel, row, update_id=1502)
    e.services.turns.accept_staged.assert_awaited_once()
    stage = e.services.turns.accept_staged.await_args.args[1]
    assert stage.thread_id == e.thread_id and stage.human_message_kwargs["human_input_response"]["option_id"] == "allow_temporary"
    async with e.db.sessions() as reader:
        act = await reader.get(TelegramActRow, identity)
        assert act.body["host"] == "example.test" and act.body["option_id"] == "allow_temporary"
        assert act.state != "done", "Only actual publication confirmation completes the action"


async def test_unwitnessed_network_request_cannot_create_a_card(network):
    e = network
    await permissions(e, 7)
    e.seed["messages"][-1]["content"]["artifact"]["human_input"]["details"]["host"] = "forged.example"
    with pytest.raises(ValueError):
        await offer(e)
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "network")))


async def test_mixed_file_remains_deliverable_after_manage_is_withdrawn(network, exports):
    from test_telegram_mixed_request_source import mixed_basis

    from app.telegram_bots.files import FileBytesBudget

    e = network
    assert exports is e
    e.seed = mixed_basis(e.record, paths=("/mnt/user-data/outputs/result.txt",), payload=network_request())
    e.services.file_bytes_budget = FileBytesBudget(50_000_000)
    e.services.model_source = ModelSource(read_output=e.coordinator.read_output, human_input_source=e.services.human_inputs.source, outbox=e.services.outbox, http_owner=e.http_client)
    await permissions(e, 7)
    item = await offer(e)
    assert item.payload["reply_basis"]["version"] == 7
    await permissions(e, 3)
    e.request.enqueue("sendDocument", result=sent(e, identity=701))
    await drain(e)
    assert e.native_reads == ["outputs/result.txt"]
    assert [call.endpoint for call in e.request.calls if call.endpoint.startswith("send")] == ["sendDocument"]
    fresh = await e.services.outbox.get(e.link, item.id)
    assert fresh.parts[0]["state"] == "sent" and fresh.parts[0]["message_ids"] == [701]
    assert fresh.parts[-1]["state"] != "sent"
    assert e.services.file_bytes_budget.used == 0


@pytest.mark.parametrize("change", ["disabled", "released", "claim", "controls", "manage", "protected", "lifetime"])
async def test_final_sdk_snapshot_cannot_send_retired_card(network, change):
    from datetime import UTC, datetime

    from sqlalchemy import event, insert

    from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow
    from deerflow.persistence.user.access import DisabledIdentityRow
    from deerflow.persistence.user.model import UserRow

    e = network
    await permissions(e, 7)
    await offer(e)
    limiter, changed = e.channel.bot.rate_limiter, []

    def disable(connection, cursor, statement, parameters, context, many):
        if "JOIN telegram_outbox" not in statement or "LEFT OUTER JOIN telegram_callbacks" not in statement or changed:
            return
        changed.append(True)
        if change == "disabled":
            connection.execute(insert(DisabledIdentityRow).values(issuer=LOCAL_ACCOUNT_ISSUER, subject=e.link.owner_user_id, disabled_at=datetime.now(UTC)))
        elif change == "released":
            connection.execute(update(UserRow).where(UserRow.id == e.link.owner_user_id).values(released_at=datetime.now(UTC)))
        elif change == "manage":
            connection.execute(update(AgentInstanceGrantRow).where(AgentInstanceGrantRow.instance_id == e.link.agent_ref).values(permissions=1))
        elif change == "protected":
            connection.execute(insert(TelegramInstancePolicyRow).values(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=datetime.now(UTC)))
        elif change == "lifetime":
            e.active[0] = False
        elif change == "claim":
            connection.execute(update(TelegramOutboxRow).where(TelegramOutboxRow.link_id == e.link.id).values(claim_id="retired-claim"))
        else:
            connection.execute(update(TelegramCallbackRow).where(TelegramCallbackRow.link_id == e.link.id).values(consumed_at=datetime.now(UTC)))

    async def waited(delay):
        limiter._short_hold_until = 0
        event.listen(e.db.engine.sync_engine, "before_cursor_execute", disable)

    limiter._sleep = waited
    limiter._short_hold_until = limiter._time() + 10
    e.request.enqueue("sendMessage", result=sent(e, identity=701))
    try:
        await drain(e)
        assert changed == [True]
        assert not any(call.endpoint == "sendMessage" for call in e.request.calls)
    finally:
        if event.contains(e.db.engine.sync_engine, "before_cursor_execute", disable):
            event.remove(e.db.engine.sync_engine, "before_cursor_execute", disable)
