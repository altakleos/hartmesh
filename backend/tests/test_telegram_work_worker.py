"""Actual default owners join SDK consent to worker-produced Work evidence.

Directory-backed prepared storage is explicit. Native containment is separately
qualified by the mandatory shipped Compose suite, never by this test.
"""

import asyncio
import json
from uuid import uuid4

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _telegram_compose_turn_support import _delivered, chat_completions_stream, install_provider_wire, render_compose_config
from _telegram_work_worker_support import WorkerProviderWire, installed_worker, propose
from sqlalchemy import func, select
from test_telegram_callback_commands import press
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots.assignment_proposal_records import validate_proposal_reply_basis
from app.telegram_bots.producer import _item
from deerflow.persistence.agent_instances.model import AgentConversationRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.run.model import RunRow
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
async def test_default_supervisor_joins_actual_proposal_consent_worker_report_and_sdk(capture_environment, monkeypatch, tmp_path):
    await asyncio.to_thread(render_compose_config, tmp_path, monkeypatch, keys={"OPENAI_API_KEY": "sk-synthetic"})
    statement = "The documentation review found no missing steps."
    operation = uuid4().hex
    proposal = ("propose_assignment", {"proposal": {"objective": "Review the guide", "success_criteria": "Report verified gaps"}})
    wire = WorkerProviderWire([], chat_completions_stream(text_value="A short title"))

    def report():
        body = json.loads(wire.lead_bodies[-1])
        context = None
        for message in reversed(body["messages"]):
            if message.get("role") != "tool":
                continue
            content = message["content"]
            text = content if isinstance(content, str) else "".join(part.get("text", "") for part in content)
            for offset, char in enumerate(text):
                if char != "{":
                    continue
                try:
                    value, _end = json.JSONDecoder().raw_decode(text[offset:])
                except ValueError:
                    continue
                for _ in range(4):
                    if not isinstance(value, dict) or "work" in value or "content" not in value:
                        break
                    value = value["content"]
                    if isinstance(value, str):
                        value = json.loads(value)
                if isinstance(value, dict) and "work" in value:
                    context = value
                    break
            if context is not None:
                break
        assert context is not None, body["messages"][-1]
        return chat_completions_stream(tool=("report_work", {"report": {"operation_id": operation, "expected_revision": context["work"]["revision"], "action": "outcome", "statement": statement}}), identity="chatcmpl-report")

    wire.replies = [chat_completions_stream(tool=proposal), chat_completions_stream(tool=("read_work_context", {}), identity="chatcmpl-context"), report, chat_completions_stream(text_value=statement, identity="chatcmpl-final")]
    install_provider_wire(monkeypatch, wire)
    async with installed_worker(capture_environment, monkeypatch, tmp_path) as e:
        first = await propose(e)
        async with e.e.sf() as reader:
            assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 0
            assert await reader.scalar(select(func.count()).select_from(AgentWorkAttemptRow)) == 0
            control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "assign_start", TelegramCallbackRow.consumed_at.is_(None)))
            if control is None:
                pytest.fail("No actual proposal control. Gateway responses:\n" + json.dumps(e.http_errors, indent=2))
            parent = await reader.get(TelegramOutboxRow, control.payload["anchor_outbox_id"])
            assert parent.state == "sent" and all(part["state"] == "sent" for part in parent.parts)
            assert parent.parts[-1]["callback_group_id"] == control.group_id
            assert parent.parts[-1]["message_ids"] == [control.message_id]
            proof = validate_proposal_reply_basis(_item(parent))
            assert proof.record.run_id == first.run_id and proof.record.turn_id == first.record.turn_id
            assert proof.output.inventory.state == "matched"
            first_run = await reader.get(RunRow, first.run_id)
            assert first_run.status == "success"
        update = press(e, 2702, control.id, message_id=control.message_id)
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        assert await e.channel.handle_update(press(e, 2703, control.id, message_id=control.message_id))
        await e.channel._drain_ingress()

        async def settled():
            while True:
                e.clock.advance(1)
                async with e.e.sf() as reader:
                    attempt = await reader.scalar(select(AgentWorkAttemptRow))
                    if attempt is not None and attempt.settled_at is not None:
                        return attempt
                await asyncio.sleep(0.1)

        attempt = await asyncio.wait_for(settled(), 45)
        await e.host.notifications.events.tick()
        await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
        async with e.e.sf() as reader:
            assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 1
            assert await reader.scalar(select(func.count()).select_from(AgentWorkAttemptRow)) == 1
            runs = list(await reader.scalars(select(RunRow)))
            assert len(runs) == 2 and {row.status for row in runs} == {"success"}
            assert attempt.run_id != first.run_id and attempt.thread_id != first_run.thread_id
            assert (await reader.get(RunRow, attempt.run_id)).thread_id == attempt.thread_id
            binding = await reader.get(AgentConversationRow, attempt.thread_id)
            assert (binding.instance_id, binding.requester_id, binding.creation_id) == (e.instance.id, e.link.owner_user_id, attempt.activation_id)
            event = await reader.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == operation))
            assert event is not None and event.actor_kind == "nonhuman" and event.work_id == attempt.work_id, wire.errors
            assert event.request["statement"] == statement and event.action == "outcome"
            assert event.actor_id == e.instance.principal.subject_id and event.request["attempt_id"] == attempt.id
            settlement = await reader.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.work_id == attempt.work_id, AgentWorkEventRow.action == "attempt_settled"))
            assert settlement.request["attempt_id"] == attempt.id and settlement.request["run_id"] == attempt.run_id
            result = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.event_id == settlement.id, TelegramOutboxRow.payload["work_event_intent"]["key"].as_string() == "review.ready"))
            assert result is not None and result.state == "sent" and result.request_id is not None
            assert result.payload["work_event_intent"]["attempt_id"] == attempt.id
            assert result.parts and all(part["state"] == "sent" and part["message_ids"] for part in result.parts)
            assert statement in "".join(part.get("visible_text", "") for part in result.parts)
            group = result.parts[-1]["callback_group_id"]
            controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group)))
            assert controls and all(row.message_id == result.parts[-1]["message_ids"][-1] and row.payload["anchor_outbox_id"] == result.id for row in controls)
        assert not wire.replies
        assert not wire.errors
        tools = [{item["function"]["name"] for item in json.loads(body)["tools"]} for body in wire.lead_bodies]
        assert "propose_assignment" in tools[0]
        assert all("propose_assignment" not in names and {"read_work_context", "report_work"} <= names for names in tools[1:])
