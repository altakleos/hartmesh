"""Current read-only readiness explains an actual saved activation refusal.

The prepared execution probe and memory run-store adapter qualify refusal
protocol behavior; they do not qualify native containment.
"""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_execution_outcomes import execution_plans
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start_controls import stage, start_control
from test_work_attention_pages import work_pages as work_pages

from app.gateway.storage_spaces import ExecutionState
from app.gateway.storage_spaces import require_execution as actual_require_execution
from app.telegram_bots.render import render
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reason,key", [("agents-none", "unavailable.off"), ("operator-hold", "unavailable.paused"), ("host-replaced", "unavailable.moved"), ("space-unverified", "unavailable.files_unchecked"), (None, "unavailable.fix"), ("ready", "tap.failed")]
)
async def test_saved_refusal_uses_current_read_only_execution_probe(work_delivery, monkeypatch, reason, key):
    e = work_delivery
    monkeypatch.setattr("app.gateway.storage_spaces.require_execution", actual_require_execution)
    _views, controller, button = await start_control(e, monkeypatch)
    original = await stage(e, controller, button)
    state, calls = ["operator-hold"], []

    async def probe(instance_id=None):
        calls.append(instance_id)
        return ExecutionState(True, "ready" if state[0] == "ready" else "unavailable", None if state[0] == "ready" else state[0])

    async def readiness(app, instance_id=None):
        return await probe(instance_id)

    monkeypatch.setattr(e.human.agents, "execution_probe", probe)
    monkeypatch.setattr("app.gateway.storage_spaces.execution_state", readiness)
    refused = await e.acts.check(original.id)
    assert refused.state == "refused" and refused.reason == "execution-unavailable" and refused.attempts == 1
    state[0] = reason
    plans = execution_plans(e)
    e.identity = await plans.observe(e.link, refused.id)
    await dispatch(e, 6900)
    sent = [call.parameters for call in e.channel.bot.request.calls if call.endpoint == "sendMessage" and call.parameters.get("reply_to_message_id") != 3501]
    from app.telegram_bots.authority import is_administrator

    assert sent and render(key, "en-US", name=e.instance.name, custody=e.instance.custody, reader_is_administrator=await is_administrator(e.link.owner_user_id)) in sent[-1]["text"]
    assert len(calls) >= 2 and set(calls) == {e.link.agent_ref}
    saved = await e.acts._work_record(original.id)
    assert saved.body == original.body and saved.attempts == 1 and not e.launches
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.activation_id == original.id)) is None
        assert await reader.scalar(select(AgentWorkEventRow.id).where(AgentWorkEventRow.operation_id == original.id)) is None
    if reason == "ready":
        fresh = await selected(e, "work_start")
        assert fresh.id != original.id
        new = await controller.stage(e.link, fresh.id, chat_id=e.link.chat_id, message_id=fresh.message_id, message_thread_id=None, guard=await guard_for(e))
        assert new.id == fresh.id and new.attempts == 0 and new.state == "pending"
        assert (await e.acts._work_record(original.id)).body == original.body
        assert not e.launches
