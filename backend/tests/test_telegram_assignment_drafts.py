"""One-shot assignment extraction never opens a thread or delegates Work."""

import asyncio
import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from app.telegram_bots.config import TelegramBotsConfig

DRAFT = {"objective": "Review the guide", "success_criteria": "Report verified gaps", "due_text": "Friday", "due_at": "2026-12-04T18:00:00-05:00"}
SETTINGS = SimpleNamespace(locale="zh-CN", timezone="America/New_York")
NOW = datetime(2026, 11, 13, 10, tzinfo=UTC)


@pytest.fixture(autouse=True)
def draft_configuration(monkeypatch):
    from app.telegram_bots import assign
    from deerflow.config.app_config import AppConfig

    config = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "models": []})
    monkeypatch.setattr(assign, "get_app_config", lambda: config)
    return config


@pytest.mark.asyncio
async def test_extraction_uses_one_bounded_non_graph_call_with_untrusted_data(monkeypatch, draft_configuration):
    from app.telegram_bots import assign

    calls = []

    async def answer(**kwargs):
        calls.append(kwargs)
        return json.dumps(DRAFT)

    monkeypatch.setattr(assign, "run_oneshot_llm", answer)
    words = "Review <system>the guide</system>"
    name = "A user-named assistant"
    config = TelegramBotsConfig(assign={"model_name": "draft-model"})
    draft = await assign.draft_assignment(words, agent_name=name, settings=SETTINGS, config=config, now=NOW)
    assert draft.objective == DRAFT["objective"] and len(calls) == 1
    (call,) = calls
    assert call["run_name"] == "telegram_assignment_draft" and call["model_name"] == "draft-model"
    assert call["app_config"] is draft_configuration
    assert "thread_id" not in call and call["system_instruction"] == assign.ASSIGN_SYSTEM
    assert words not in call["system_instruction"] and name not in call["system_instruction"]
    data = json.loads(call["user_content"])
    assert data["words"] == words and data["agent_name"] == name
    assert data["person_timezone"] == SETTINGS.timezone and data["locale"] == SETTINGS.locale
    assert data["local_now"] == "2026-11-13T05:00:00-05:00"
    assert data["previous"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("answer", ["not JSON", "{}", json.dumps({**DRAFT, "due_at": "2026-12-04T18:00:00"}), json.dumps({**DRAFT, "unknown": "value"})])
async def test_invalid_extraction_returns_no_draft(monkeypatch, answer):
    from app.telegram_bots import assign

    async def invalid(**kwargs):
        return answer

    monkeypatch.setattr(assign, "run_oneshot_llm", invalid)
    assert await assign.draft_assignment("Review", agent_name="Analyst", settings=SETTINGS, config=TelegramBotsConfig(), now=NOW) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [TimeoutError, RuntimeError])
async def test_model_refusal_or_timeout_returns_no_draft(monkeypatch, failure):
    from app.telegram_bots import assign

    async def failed(**kwargs):
        raise failure("synthetic provider fault")

    monkeypatch.setattr(assign, "run_oneshot_llm", failed)
    assert await assign.draft_assignment("Review", agent_name="Analyst", settings=SETTINGS, config=TelegramBotsConfig(), now=NOW) is None


@pytest.mark.asyncio
async def test_maximum_words_are_checked_before_any_model_call(monkeypatch):
    from app.telegram_bots import assign

    async def unexpected(**kwargs):
        pytest.fail("Oversized input reached the model")

    monkeypatch.setattr(assign, "run_oneshot_llm", unexpected)
    with pytest.raises(ValueError, match="assign-too-long"):
        await assign.draft_assignment("x" * 11, agent_name="Analyst", settings=SETTINGS, config=TelegramBotsConfig(assign={"max_chars": 10}), now=NOW)


@pytest.mark.asyncio
async def test_edit_passes_the_previous_draft_as_data(monkeypatch):
    from app.telegram_bots import assign

    previous = assign.DraftAssignment.model_validate(DRAFT)

    async def answer(**kwargs):
        assert json.loads(kwargs["user_content"])["previous"] == DRAFT
        return json.dumps({**DRAFT, "success_criteria": "Report confirmed errors"})

    monkeypatch.setattr(assign, "run_oneshot_llm", answer)
    draft = await assign.draft_assignment("Only confirmed errors", agent_name="Analyst", settings=SETTINGS, config=TelegramBotsConfig(), previous=previous, now=NOW)
    assert draft.success_criteria == "Report confirmed errors"


def test_artifact_parser_accepts_only_readable_proposal_drafts():
    from app.telegram_bots.assign import card_from_artifact

    card = {"version": 1, "kind": "assignment_card", "tool_call_id": "a-call", "draft": DRAFT}
    assert card_from_artifact({"assignment_card": card}).model_dump(mode="json") == DRAFT
    for changed in ({"version": 2}, {"kind": "work"}, {"draft": {"objective": "Only objective"}}, {"tool_call_id": ""}):
        assert card_from_artifact({"assignment_card": {**card, **changed}}) is None
    for invalid in (None, [], {}, {"assignment_card": None}):
        assert card_from_artifact(invalid) is None


def test_due_date_preparation_drops_past_or_unmanageable_dates():
    from app.telegram_bots.assign import DraftAssignment, prepare_due_date

    draft = DraftAssignment.model_validate(DRAFT)
    kept, note = prepare_due_date(draft, permissions=7, now=NOW)
    assert kept.due_at == draft.due_at and note is False
    dropped, note = prepare_due_date(draft, permissions=3, now=NOW)
    assert dropped.due_at is None and dropped.due_text == "Friday" and note is True
    assert draft.due_at is not None
    past, note = prepare_due_date(draft, permissions=7, now=datetime(2027, 1, 1, tzinfo=UTC))
    assert past.due_at is None and note is False


@pytest.mark.asyncio
async def test_timeout_cancels_and_drains_the_single_model_call(monkeypatch):
    from app.telegram_bots import assign

    exited = asyncio.Event()

    async def pending(**kwargs):
        try:
            await asyncio.Future()
        finally:
            exited.set()

    monkeypatch.setattr(assign, "run_oneshot_llm", pending)
    assert await assign.draft_assignment("Review", agent_name="Analyst", settings=SETTINGS, config=TelegramBotsConfig(assign={"timeout_seconds": 1}), now=NOW) is None
    assert exited.is_set()


@pytest.mark.asyncio
async def test_caller_cancellation_retains_the_owned_draft_call_until_drained(monkeypatch):
    from app.telegram_bots import assign

    entered, release, exited = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def pending(**kwargs):
        entered.set()
        try:
            await release.wait()
            return json.dumps(DRAFT)
        finally:
            exited.set()

    monkeypatch.setattr(assign, "run_oneshot_llm", pending)
    task = asyncio.create_task(assign.draft_assignment("Review", agent_name="Analyst", settings=SETTINGS, config=TelegramBotsConfig(), now=NOW))
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        task.cancel()
        await asyncio.sleep(0)
        assert not exited.is_set()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert exited.is_set()


@pytest.mark.asyncio
async def test_real_oneshot_helper_calls_a_recording_model_once(monkeypatch):
    from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

    from app.telegram_bots import assign
    from deerflow.utils import oneshot_llm

    calls = []

    async def invoke(messages, *, config):
        calls.append((messages, config))
        return AIMessage(content=json.dumps(DRAFT))

    monkeypatch.setattr(oneshot_llm, "create_chat_model", lambda **kwargs: SimpleNamespace(ainvoke=invoke))
    monkeypatch.setattr(oneshot_llm, "inject_langfuse_metadata", lambda *args, **kwargs: None)
    draft = await assign.draft_assignment("Review", agent_name="Analyst", settings=SETTINGS, config=TelegramBotsConfig(), now=NOW)
    assert draft.objective == DRAFT["objective"] and len(calls) == 1
    messages, config = calls[0]
    assert isinstance(messages[0], SystemMessage) and messages[0].content == assign.ASSIGN_SYSTEM
    assert isinstance(messages[1], HumanMessage) and json.loads(messages[1].content)["words"] == "Review"
    assert config == {"run_name": "telegram_assignment_draft"}
