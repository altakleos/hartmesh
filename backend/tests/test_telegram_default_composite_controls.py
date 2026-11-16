"""Installed request controls are derived from real Native request records."""

import asyncio
from uuid import uuid4

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _telegram_compose_turn_support import _delivered, render_compose_config
from _telegram_work_worker_support import installed_worker
from test_telegram_retained_management_refusals import button, tap
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from deerflow.agent_instances.human_input import HumanInput
from deerflow.agent_instances.human_input_contract import CreateRequest
from deerflow.agent_instances.work import AgentWork
from deerflow.agent_instances.work_contract import DelegateWork
from deerflow.spaces.contract import PrincipalRef


async def decision_confirmation(e):
    human, actor = HumanInput(AgentWork(e.app.state.agent_instances)), PrincipalRef("human", e.link.owner_user_id)
    work = await human.work.delegate(actor=actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Review the guide", success_criteria="Report verified gaps"))
    request = await human.create(
        actor=actor,
        instance_id=e.instance.id,
        work_id=work["id"],
        request=CreateRequest(
            operation_id=uuid4().hex,
            expected_work_revision=work["revision"],
            expected_assignment_revision=work["assignment_revision"],
            purpose="decision",
            question="Choose a guide",
            reason="A current guide is required",
            expected_response="An explicit choice",
            choices=["Use the current guide"],
            recipient_id=e.link.owner_user_id,
        ),
    )
    await e.host.work_callbacks.outcomes.plans.plan(e.link, request_id=request["id"], dedupe_key="actual-decision:" + request["id"])
    await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
    await tap(e, await button(e, "decide_choice"), 7060)
    await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
    return work, request, await button(e, "decide_confirm_resume")


@pytest.mark.asyncio
async def test_default_decision_offers_explicit_resume(capture_environment, monkeypatch, tmp_path):
    await asyncio.to_thread(render_compose_config, tmp_path, monkeypatch, keys={"OPENAI_API_KEY": "sk-synthetic"})
    async with installed_worker(capture_environment, monkeypatch, tmp_path) as e:
        _work, _request, control = await decision_confirmation(e)
        assert control.kind == "decide_confirm_resume" and control.message_id is not None


async def voice_confirmation(e, monkeypatch):
    from test_telegram_voice_notes import Transcribing, note

    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow

    monkeypatch.setattr("app.telegram_bots.inbox._now", e.clock.now)
    human, actor = HumanInput(AgentWork(e.app.state.agent_instances)), PrincipalRef("human", e.link.owner_user_id)
    work = await human.work.delegate(actor=actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Review the guide", success_criteria="Report verified gaps"))
    request = await human.create(
        actor=actor,
        instance_id=e.instance.id,
        work_id=work["id"],
        request=CreateRequest(
            operation_id=uuid4().hex,
            expected_work_revision=work["revision"],
            expected_assignment_revision=work["assignment_revision"],
            purpose="information",
            question="Which account should be used?",
            reason="An account must be selected",
            expected_response="The account name",
            recipient_id=e.link.owner_user_id,
        ),
    )
    identity = await e.host.work_callbacks.outcomes.plans.plan(e.link, request_id=request["id"], dedupe_key="actual-question:" + request["id"])
    await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
    async with e.e.sf() as reader:
        question = await reader.get(TelegramOutboxRow, identity)
        assert question.state == "sent" and question.parts[-1]["message_ids"]
        anchor = question.parts[-1]["message_ids"][-1]
    transcriber = Transcribing(text="Use the current account.")
    monkeypatch.setattr("app.telegram_bots.voice.resolve_transcriber", lambda config, **kwargs: transcriber)

    async def audio(url, *, ceiling, max_bytes, before_dispatch, on_failed_close=None):
        await before_dispatch()
        return b"opus-audio"

    monkeypatch.setattr(e.normal, "read_file_capped", audio, raising=False)
    await e.host.settings.update(e.link.owner_user_id, voice_agreed_for="openai", voice_agreed_at=e.clock.now())
    e.normal.enqueue("getFile", result={"file_id": "voice-7061", "file_unique_id": "unique-7061", "file_size": 10, "file_path": "voice/note.oga"})
    assert await e.channel.handle_update(note(e, 7061, quoted=anchor))
    await e.channel._drain_ingress()
    await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
    async with e.e.sf() as reader:
        incoming = await reader.get(TelegramInboundMarkRow, (e.link.id, 7061, e.link.chat_id))
        assert incoming.route_kind == "work_answer" and not incoming.operation_ids
    assert len(transcriber.calls) == 1
    return work, request, await button(e, "voice_save_resume")


@pytest.mark.asyncio
async def test_default_voice_offers_explicit_resume(capture_environment, monkeypatch, tmp_path):
    await asyncio.to_thread(render_compose_config, tmp_path, monkeypatch, keys={"OPENAI_API_KEY": "sk-synthetic"})
    async with installed_worker(capture_environment, monkeypatch, tmp_path) as e:
        _work, _request, control = await voice_confirmation(e, monkeypatch)
        assert control.kind == "voice_save_resume" and control.message_id is not None
