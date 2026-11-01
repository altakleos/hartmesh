"""A person's message reaches the model and the answer reaches them, on what Compose ships.

The other turn tests choose their own configuration. They load no plugin, reply
with plain strings and work without a Space, and Telegram could not answer anyone
on Compose while they all passed. See `_telegram_compose_turn_support.py`.
"""

import logging

import httpx
import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _telegram_compose_turn_support import ProviderWire, anthropic_stream, chat_completions_stream, drive_message, install_provider_wire, produce_file, render_compose_config, responses_stream
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.gateway import chat_files, chat_upload_ingestion
from app.gateway.channel_execution import CHANNEL_EXECUTION_BUSY_CODE
from deerflow.spaces.chat_paths import ChatFilesFull

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


async def turn(e, monkeypatch, tmp_path, *, keys, replies, auxiliary, tweak=None, prompts=("What is two plus two?",), document=None):
    wire = ProviderWire(replies, auxiliary)
    install_provider_wire(monkeypatch, wire)
    render_compose_config(tmp_path, monkeypatch, keys=keys, tweak=tweak)
    facts = await drive_message(e, monkeypatch, tmp_path, prompts=prompts, document=document)
    assert facts.plugins, "This proves nothing about Compose unless its plugins are loaded"
    assert not wire.replies, "Every scripted reply must have been asked for"
    assert set(facts.runs) == {"success"}
    assert set(facts.turn_runs) == set(facts.run_ids), "A chat's log lines name its run, so the turn has to know it"
    assert not any(FAILURE in (shown or "") for _endpoint, shown in facts.sent), facts.sent
    facts.lead_bodies = wire.lead_bodies
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
async def test_a_file_sent_to_the_bot_lands_in_the_persons_space_and_the_agent_sees_it(capture_environment, monkeypatch, tmp_path):
    facts = await turn(
        capture_environment,
        monkeypatch,
        tmp_path,
        keys=OPENAI,
        replies=[chat_completions_stream(text_value="Three.")],
        auxiliary=gpt4_auxiliary(),
        prompts=("Add these up.",),
        document=("input.csv", b"a,b\n1,2\n"),
    )
    assert facts.sent == [("sendMessage", "Three.")]
    assert [path.read_bytes() for path in facts.in_spaces] == [b"a,b\n1,2\n"], "On Compose a conversation's files live in the person's Space"
    assert facts.outside_spaces == [], "A file on the shared disk is one the agent cannot reach"
    assert "/mnt/user-data/uploads/input.csv" in facts.lead_bodies[0], "The model is told where the file is"


def no_room(self, expected_bytes, *, minimum_inodes=2):
    raise ChatFilesFull()


async def fault_the_route_does_not_map(self):
    raise RuntimeError("synthetic")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "target, attribute, fault, notice",
    [
        (chat_files.QualifiedUploadStage, "check_capacity", no_room, "Your files are full, so input.csv was not saved."),
        (chat_upload_ingestion.ChatUploadSession, "finalize", fault_the_route_does_not_map, "input.csv could not be saved."),
    ],
    ids=["no room", "no readable answer"],
)
async def test_a_file_that_was_not_saved_is_named_and_the_message_is_still_answered(capture_environment, monkeypatch, tmp_path, target, attribute, fault, notice):
    monkeypatch.setattr(target, attribute, fault)
    facts = await turn(
        capture_environment,
        monkeypatch,
        tmp_path,
        keys=OPENAI,
        replies=[chat_completions_stream(text_value="Three.")],
        auxiliary=gpt4_auxiliary(),
        prompts=("Add these up.",),
        document=("input.csv", b"a,b\n1,2\n"),
    )
    assert ("sendMessage", "Three.") in facts.sent, "The notice and the answer race, and neither waits for the other"
    assert any((shown or "").startswith(notice) for _endpoint, shown in facts.sent), facts.sent
    assert facts.in_spaces == [] and "/mnt/user-data/uploads/input.csv" not in facts.lead_bodies[0]


@pytest.mark.asyncio
async def test_a_message_that_waits_for_its_conversation_saves_its_file_once(capture_environment, monkeypatch, tmp_path):
    asgi, refusals = httpx.ASGITransport, [{"detail": {"code": CHANNEL_EXECUTION_BUSY_CODE}}]

    class Busy(asgi):
        async def handle_async_request(self, request):
            if request.url.path.endswith("/runs/stream") and refusals:
                return httpx.Response(409, json=refusals.pop())
            return await super().handle_async_request(request)

    monkeypatch.setattr(httpx, "ASGITransport", Busy)
    facts = await turn(
        capture_environment,
        monkeypatch,
        tmp_path,
        keys=OPENAI,
        replies=[chat_completions_stream(text_value="Three.")],
        auxiliary=gpt4_auxiliary(),
        prompts=("Add these up.",),
        document=("input.csv", b"a,b\n1,2\n"),
    )
    assert not refusals and facts.sent[-1] == ("sendMessage", "Three.")
    assert [path.name for path in facts.in_spaces] == ["input.csv"], "Every copy would count against the person's room, and the agent would see them all"
    assert "/mnt/user-data/uploads/input.csv" in facts.lead_bodies[0]


@pytest.mark.asyncio
async def test_claude_asks_a_question(capture_environment, monkeypatch, tmp_path):
    question = ("ask_clarification", {"question": "Which one do you mean?", "options": ["First", "Second"]})
    facts = await turn(capture_environment, monkeypatch, tmp_path, keys=ANTHROPIC, replies=[anthropic_stream(thinking="Unclear.", tool=question)], auxiliary=claude_auxiliary())
    assert facts.sent == [("sendMessage", "Which one do you mean?\n\n1. First\n2. Second")]
