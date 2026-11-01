"""A person's message reaches the model and the answer reaches them, on what Compose ships.

The other turn tests choose their own configuration. They load no plugin, reply
with plain strings and work without a Space, and Telegram could not answer anyone
on Compose while they all passed. See `_telegram_compose_turn_support.py`.
"""

import logging

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _telegram_compose_turn_support import ProviderWire, anthropic_stream, chat_completions_stream, drive_message, install_provider_wire, produce_file, render_compose_config, responses_stream
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

OPENAI = {"OPENAI_API_KEY": "sk-synthetic"}
ANTHROPIC = {"ANTHROPIC_API_KEY": "sk-ant-synthetic"}
SEARCH = ("web_search", {"query": "synthetic"})
FAILURE = "Something went wrong"


@pytest.fixture(autouse=True)
def nothing_stops_on_the_way(caplog):
    """A live view can die and the answer still arrive, so an answer alone proves too little."""
    yield
    stopped = [record.getMessage() for record in caplog.records if record.levelno >= logging.WARNING and record.getMessage().startswith("telegram_bots chat_")]
    assert not stopped


def gpt4_auxiliary():
    return chat_completions_stream(text_value="A short title")


def claude_auxiliary():
    return anthropic_stream(text_value="A short title", message_id="msg_aux")


def only_responses(document):
    document["models"] = [model for model in document["models"] if model["name"] == "gpt-5-responses"]


async def turn(e, monkeypatch, tmp_path, *, keys, replies, auxiliary, tweak=None, prompts=("What is two plus two?",)):
    wire = ProviderWire(replies, auxiliary)
    install_provider_wire(monkeypatch, wire)
    render_compose_config(tmp_path, monkeypatch, keys=keys, tweak=tweak)
    facts = await drive_message(e, monkeypatch, tmp_path, prompts=prompts)
    assert facts.plugins, "This proves nothing about Compose unless its plugins are loaded"
    assert not wire.replies, "Every scripted reply must have been asked for"
    assert set(facts.runs) == {"success"}
    assert set(facts.turn_runs) == set(facts.run_ids), "A chat's log lines name its run, so the turn has to know it"
    assert not any(FAILURE in (shown or "") for _endpoint, shown in facts.sent), facts.sent
    return facts


@pytest.mark.asyncio
async def test_the_default_model_answers(capture_environment, monkeypatch, tmp_path):
    facts = await turn(capture_environment, monkeypatch, tmp_path, keys=OPENAI, replies=[chat_completions_stream(text_value="Four.")], auxiliary=gpt4_auxiliary())
    assert facts.sent == [("sendMessage", "Four.")]


@pytest.mark.asyncio
async def test_claude_answers_with_thinking_on_and_keeps_it_private(capture_environment, monkeypatch, tmp_path):
    facts = await turn(capture_environment, monkeypatch, tmp_path, keys=ANTHROPIC, replies=[anthropic_stream(thinking="Private sum.", text_value="Four.")], auxiliary=claude_auxiliary())
    assert facts.sent == [("sendMessage", "Four.")]
    assert "Private sum." not in facts.bot_calls


@pytest.mark.asyncio
async def test_claude_answers_a_second_message_after_thinking_in_the_first(capture_environment, monkeypatch, tmp_path):
    replies = [anthropic_stream(thinking="Sum.", text_value="Four."), anthropic_stream(thinking="Sum.", text_value="Five.", message_id="msg_2")]
    facts = await turn(capture_environment, monkeypatch, tmp_path, keys=ANTHROPIC, replies=replies, auxiliary=claude_auxiliary(), prompts=("What is two plus two?", "And plus one?"))
    assert facts.sent == [("sendMessage", "Four."), ("sendMessage", "Five.")]


@pytest.mark.asyncio
async def test_claude_answers_after_calling_a_tool(capture_environment, monkeypatch, tmp_path):
    replies = [anthropic_stream(thinking="Search.", text_value="Let me check.", tool=SEARCH), anthropic_stream(thinking="Done.", text_value="Found it.", message_id="msg_2")]
    facts = await turn(capture_environment, monkeypatch, tmp_path, keys=ANTHROPIC, replies=replies, auxiliary=claude_auxiliary())
    assert facts.sent == [("sendMessage", "Found it.")]


@pytest.mark.asyncio
async def test_gpt5_responses_answers_after_calling_a_tool(capture_environment, monkeypatch, tmp_path):
    replies = [responses_stream(tool=SEARCH), responses_stream(text_value="Found it.", identity="resp_2")]
    facts = await turn(capture_environment, monkeypatch, tmp_path, keys=OPENAI, tweak=only_responses, replies=replies, auxiliary=gpt4_auxiliary())
    assert facts.sent == [("sendMessage", "Found it.")]


@pytest.mark.asyncio
async def test_claude_delivers_a_file_made_in_the_first_turn_of_a_conversation(capture_environment, monkeypatch, tmp_path):
    reply = produce_file(tmp_path / "spaces", "report.txt", b"Actual generated file", anthropic_stream(thinking="Done.", text_value="Here is your report."))
    facts = await turn(capture_environment, monkeypatch, tmp_path, keys=ANTHROPIC, replies=[reply], auxiliary=claude_auxiliary())
    assert facts.sent == [("sendDocument", "Here is your report.")]


@pytest.mark.asyncio
async def test_claude_asks_a_question(capture_environment, monkeypatch, tmp_path):
    question = ("ask_clarification", {"question": "Which one do you mean?", "options": ["First", "Second"]})
    facts = await turn(capture_environment, monkeypatch, tmp_path, keys=ANTHROPIC, replies=[anthropic_stream(thinking="Unclear.", tool=question)], auxiliary=claude_auxiliary())
    assert facts.sent == [("sendMessage", "Which one do you mean?\n\n1. First\n2. Second")]
