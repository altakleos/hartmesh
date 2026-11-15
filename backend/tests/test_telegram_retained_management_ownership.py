"""Real management receipts and late ownership prevent a false refusal reply."""

import asyncio
from contextlib import suppress

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _telegram_compose_turn_support import render_compose_config
from _telegram_work_worker_support import installed_worker
from sqlalchemy import select
from test_telegram_retained_management_refusals import management_preview, tap
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.render import render
from deerflow.agent_instances.human_input import HumanInput
from deerflow.agent_instances.work import AgentWork
from deerflow.agent_instances.work_contract import WorkCommand
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramOutboxRow
from deerflow.spaces.contract import PrincipalRef

MANAGEMENT_OWNERS = ["restore", "event-before", "event-after", "dispatch-history"]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", MANAGEMENT_OWNERS)
async def test_retained_management_preserves_real_owners(capture_environment, monkeypatch, tmp_path, change):
    await asyncio.to_thread(render_compose_config, tmp_path, monkeypatch, keys={"OPENAI_API_KEY": "sk-synthetic"})
    async with installed_worker(capture_environment, monkeypatch, tmp_path) as e:
        work, control = await management_preview(e, "cancel", monkeypatch, reserve=change == "dispatch-history")
        check, changed, bodies = e.host.acts.check, [], []

        async def current(identity, **kwargs):
            if identity == control.id and not bodies:
                async with e.e.sf.begin() as writer:
                    act = await writer.get(TelegramActRow, identity)
                    bodies.append(act.body)
                    if change != "event-before":
                        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
            return await check(identity, **kwargs)

        fixed, read = e.host.work_saved_refusals, e.host.work_saved_refusals.read_part

        async def late(captured, **kwargs):
            result = await read(captured, **kwargs)
            if not changed:
                changed.append(change)
                if change == "restore":
                    async with e.e.sf.begin() as writer:
                        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 7
                elif change == "event-after":
                    async with e.e.sf.begin() as writer:
                        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 7
                    human = HumanInput(AgentWork(e.app.state.agent_instances))
                    await human.work.command(actor=PrincipalRef("human", e.link.owner_user_id), instance_id=e.instance.id, work_id=work["id"], request=WorkCommand.model_validate(bodies[0]), can_stop=False)
                    async with e.e.sf.begin() as writer:
                        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
                elif change == "dispatch-history":
                    async with e.e.sf.begin() as writer:
                        act = await writer.get(TelegramActRow, control.id)
                        assert act.stop_dispatch is not None and act.stop_dispatch["attempts"] == 0
                        # Negative corruption: a recorded entered follow-up,
                        # even on a refused command, cannot become no-effect.
                        act.stop_dispatch = {**act.stop_dispatch, "attempts": 1, "ended_from": "ready"}
            return result

        monkeypatch.setattr(e.host.acts, "check", current)
        monkeypatch.setattr(fixed, "read_part", late)
        before = len(e.normal.calls)
        await tap(e, control, 7030)
        if change == "event-before":
            async with e.e.sf.begin() as writer:
                act = await writer.get(TelegramActRow, control.id)
                event = await writer.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == control.id))
                assert act.state == "done" and event is not None and event.action == "cancel"
                # Only the journal is damaged. The genuine service event wins.
                act.state, act.reason, act.result = "refused", "grant-required", None
                (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
                canonical = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_management_intent"]["act_id"].as_string() == control.id))
                identity = canonical.id
            with suppress(ProductSourceRetired):
                await e.host.notifications.chat.management.observe(e.link, identity, guard=e.host.notifications.chat.guard(e.channel, e.link))
        for _ in range(4):
            e.clock.advance(5)
            await e.host.sender.tick()
            await e.host.sender.drain()
        sent = [call.parameters["text"] for call in e.normal.calls[before:] if call.endpoint == "sendMessage"]
        assert render("denied.withdraw", "en-US") not in sent, sent
        async with e.e.sf() as reader:
            act = await reader.get(TelegramActRow, control.id)
            event = await reader.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == control.id))
            assert act.body == bodies[0]
            assert (event is not None) is (change in {"event-before", "event-after"})
            if change == "event-before":
                assert act.state == "done" and act.result is not None
            else:
                assert changed == [change]
            if change == "dispatch-history":
                native = await reader.get(AgentWorkAttemptRow, e.native_original.attempt_id)
                assert native.status == "starting" and native.run_id is None
