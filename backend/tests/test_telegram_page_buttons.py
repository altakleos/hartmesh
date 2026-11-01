"""Page offers become buttons from the saved output proof, through the actual SDK."""

import asyncio
import json
import secrets
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _telegram_bot_test_support import ManualClock, RecordingRequest
from _telegram_human_input_test_support import request as question
from _telegram_repository_test_support import db as db
from pydantic import ValidationError
from sqlalchemy import select
from test_output_inventory import ROOT_EPOCH, inventory_document
from test_telegram_bot_handlers import sent
from test_telegram_callback_commands import drain
from test_telegram_file_source import record as file_record
from test_telegram_human_input_sdk import ui as ui
from test_telegram_publication_authority import attempt_environment as attempt_environment
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment
from test_telegram_publication_custody import publication_environment as publication_environment
from test_telegram_publication_primary import _managed_turn, _output
from test_telegram_streamed_primary import cancelled, shown
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.gateway.run_output import RunOutputProjectionV2
from app.telegram_bots import pages as pages_module
from app.telegram_bots.adapter import TelegramBotChannel
from app.telegram_bots.config import TelegramBotsConfig
from app.telegram_bots.file_source import prepare_file_reply, qualify_file_output
from app.telegram_bots.model_source import ModelSource, prepare_verified_reply
from app.telegram_bots.pages import page_buttons
from app.telegram_bots.producer import Outbox, OutboxSender, SendingPart, _item
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow
from deerflow.runtime.runs.output_inventory import PageInventoryDescriptor, StructuredInventoryDescriptor, build_event_inventory_evidence, content_items, delivery_items, offered_pages, output_receipt_digest

BASE = "https://hartmesh.example"
MEMORY = BASE + "/workspace?settings=memory"
CONVERSATIONS = BASE + "/workspace/chats"
SCHEDULED = BASE + "/workspace/scheduled-tasks"


def link(kind="lead_agent", ref="lead_agent"):
    return SimpleNamespace(agent_kind=kind, agent_ref=ref)


def page_item(key, index=0):
    return {"kind": "page", "key": key, "witness": {"kind": "root_snapshot", "root_epoch": ROOT_EPOCH, "message_id": f"tool-{index}"}}


def artifact_item(carrier="message_artifact"):
    return {"kind": "structured", "carrier": carrier, "value_sha256": "a" * 64, "witness": {"kind": "root_snapshot", "root_epoch": ROOT_EPOCH, "message_id": "tool-9"}}


def projection(record, *, keys=(), text="**synthetic answer**", file=None, structured=()):
    output = _output(record, text=text)
    receipt = output.output_evidence
    document = inventory_document(version=2, owner_user_id=receipt.owner_user_id, thread_id=receipt.thread_id, run_id=receipt.run_id, input_message_id=receipt.input_message_id, output_receipt_sha256=output_receipt_digest(receipt))
    document.pop("scan")
    document["output_view"] = {"kind": "no_admitted_output_view", "scope": {"version": 1, "kind": "managed_output_routing", "virtual_root": "/mnt/user-data/outputs"}}
    document["items"] = [page_item(key, index) for index, key in enumerate(keys)]
    if file is not None:
        document["items"].append({"kind": "file", "path": file, "surfaces": ["presentation_reference"], "witnesses": [{"kind": "root_snapshot", "root_epoch": ROOT_EPOCH, "message_id": "file-tool"}]})
    document["items"].extend(artifact_item(carrier) for carrier in structured)
    evidence = build_event_inventory_evidence(receipt, status=output.run_status, inventory_event_seq=9, document=document)
    return RunOutputProjectionV2.model_validate({"version": 2, "output": output, "inventory": {"state": "matched", "evidence": evidence, "document": document, "reason": None}})


@pytest.fixture(autouse=True)
def public_address(monkeypatch):
    monkeypatch.setattr(pages_module, "_base_url", lambda: BASE)


def test_buttons_are_the_first_three_offers_and_a_page_with_no_address_is_left_out():
    assert page_buttons(("memory", "conversations", "scheduled", "telegram"), link()) == [("label.open_memory", MEMORY), ("label.open_conversations", CONVERSATIONS), ("label.open_scheduled", SCHEDULED)]
    assert page_buttons(("memory", "scheduled"), link("custom_agent", "reader")) == [("label.open_scheduled", SCHEDULED)], "Memory has no page on a custom agent's bot, and no other page takes its place"
    assert page_buttons(("addEmployee",), link()) == []
    assert page_buttons(("addEmployee",), link(), reader_is_administrator=True) == [("label.add_employee", BASE + "/workspace/employees")]
    assert page_buttons(("nonsense",), link()) == []


def test_no_public_address_means_no_buttons(monkeypatch):
    monkeypatch.setattr(pages_module, "_base_url", lambda: None)
    assert page_buttons(("memory",), link()) == []


def test_an_offer_can_only_name_a_key():
    with pytest.raises(ValidationError):
        PageInventoryDescriptor.model_validate(page_item("https://elsewhere.example"))


async def deliver(e, monkeypatch, record, output, *, replies=1, script=None):
    """Plan the actual reply and send it through the real SDK adapter."""
    clock, request, updates = ManualClock(), RecordingRequest(), RecordingRequest()
    request.enqueue_get_me(bot_id=e.link.bot_id)
    if script is not None:
        script(request)
    for number in range(replies):
        request.enqueue("sendMessage", result={"message_id": 501 + number, "date": 1_893_456_000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Recorded model text"})
    monkeypatch.setattr("deerflow.config.agents_config.load_agent_config", lambda *args, **kwargs: SimpleNamespace(name=e.link.agent_ref))
    box = Outbox(e.sessions, clock=clock.now)
    identity = await box.plan_verified_publication_reply(record, output=output, guard=e.guard_factory())
    async with e.sessions.begin() as session:
        for row in await session.scalars(select(TelegramOutboxRow)):
            if row.id != identity:
                row.state = "sent"

    async def read(original, current_link):
        return output

    async def protect_notice(context):
        return False

    services = SimpleNamespace(
        send_allowed=lambda: True,
        config=e.db.config,
        links=e.links,
        settings=e.db.settings,
        outbox=box,
        tls_context=object(),
        request_factory=lambda **kwargs: request if kwargs["connection_pool_size"] == 8 else updates,
        protect_notice=protect_notice,
        eligibility=e.live.authority.delivery,
        model_source=ModelSource(read_output=read),
        product_name="HartMesh",
    )
    channel = await asyncio.to_thread(TelegramBotChannel, link=e.link, token=str(e.link.bot_id) + ":" + secrets.token_urlsafe(30), supervisor=services, monotonic=clock.monotonic, sleep=AsyncMock())
    services.channel = lambda link_id: channel if link_id == e.link.id else None
    sender = OutboxSender(box, run_id="page-test-process", deliver_part=lambda *, link_id, **scope: channel.send_part(**scope), eligibility=e.live.authority.delivery)
    try:
        await channel.start()
        for _ in range(4):
            clock.advance(2)
            await sender.tick()
            await sender.drain()
        return [call for call in request.calls if call.endpoint == "sendMessage"]
    finally:
        await sender.stop()
        await channel.stop()


def buttons(call):
    markup = call.parameters.get("reply_markup")
    markup = json.loads(markup) if isinstance(markup, str) else markup
    return [(button["text"], button["url"]) for row in (markup or {}).get("inline_keyboard", ()) for button in row]


@pytest.mark.asyncio
async def test_at_most_three_page_buttons(publication_environment, monkeypatch):
    e = publication_environment
    record = await _managed_turn(e)
    output = projection(record, keys=("conversations", "conversations", "scheduled", "account", "telegram"), text="word " * 1000)
    sends = await deliver(e, monkeypatch, record, output, replies=2)
    assert len(sends) == 2, "The answer was split"
    assert buttons(sends[0]) == []
    assert buttons(sends[1]) == [("Open conversations", CONVERSATIONS), ("Open scheduled tasks", SCHEDULED), ("Open HartMesh", BASE + "/workspace?settings=account")]


@pytest.mark.asyncio
async def test_model_text_cannot_supply_the_address(publication_environment, monkeypatch):
    e = publication_environment
    record = await _managed_turn(e)
    sends = await deliver(e, monkeypatch, record, projection(record, keys=("scheduled",), text="Open https://elsewhere.example/tasks"), replies=1)
    assert buttons(sends[0]) == [("Open scheduled tasks", SCHEDULED)]


@pytest.mark.asyncio
async def test_a_refused_address_becomes_the_last_line_and_nothing_is_lost(publication_environment, monkeypatch):
    e = publication_environment
    record = await _managed_turn(e)

    def refuse(request):
        request.enqueue("sendMessage", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: inline keyboard button URL 'http://localhost' is invalid"})

    sends = await deliver(e, monkeypatch, record, projection(record, keys=("account",), text="That is done on the web."), replies=1, script=refuse)
    address = BASE + "/workspace?settings=account"
    assert len(sends) == 2 and buttons(sends[0]) == [("Open HartMesh", address)]
    assert buttons(sends[1]) == [] and sends[1].parameters["text"] == "That is done on the web.\n" + address


@pytest.mark.asyncio
async def test_no_public_address_leaves_the_sentence_alone(publication_environment, monkeypatch):
    e = publication_environment
    monkeypatch.setattr(pages_module, "_base_url", lambda: None)
    record = await _managed_turn(e)
    sends = await deliver(e, monkeypatch, record, projection(record, keys=("scheduled",), text="That is done on the web."), replies=1)
    assert buttons(sends[0]) == [] and sends[0].parameters["text"] == "That is done on the web."


@pytest.mark.asyncio
async def test_pages_alone_do_not_make_a_text_reply_a_file_reply(publication_environment):
    e = publication_environment
    record = await _managed_turn(e)
    output = projection(record, keys=("memory",))
    assert offered_pages(output.inventory.document) == ("memory",)
    basis, parts = prepare_verified_reply(record, output)
    assert basis["version"] == 2 and len(parts) == 1, "The saved proof keeps the whole inventory; nothing is filtered"
    assert basis["outcome"]["projection"]["inventory"]["document"]["items"][0]["kind"] == "page"
    with pytest.raises(ValueError):
        qualify_file_output(record, output)


def test_a_file_reply_keeps_its_answer_as_text_so_the_buttons_have_a_home():
    record = file_record()
    path = "/mnt/user-data/outputs/report.txt"
    files = [{"path": path, "filename": "report.txt", "size": 10, "mime_type": "text/plain", "state": "ready"}]
    without, _ = prepare_file_reply(record, projection(record, file=path), files=files, capacity=TelegramBotsConfig().files.in_memory_max_bytes, max_bytes=TelegramBotsConfig().files.max_outbound_bytes)
    assert [part["kind"] for part in without["parts"]] == ["document"] and "caption_html" in without["parts"][0], "Without an offer a short answer is the caption"
    offered, _ = prepare_file_reply(record, projection(record, keys=("memory",), file=path), files=files, capacity=TelegramBotsConfig().files.in_memory_max_bytes, max_bytes=TelegramBotsConfig().files.max_outbound_bytes)
    assert [part["kind"] for part in offered["parts"]] == ["text", "document"] and "caption_html" not in offered["parts"][1]


@pytest.mark.asyncio
async def test_a_question_keeps_its_choices_and_gains_the_page_buttons(ui):
    e = ui
    await e.offer(question(), pages=("memory",))
    e.request.enqueue("sendMessage", result=sent(e, identity=701))
    await drain(e)
    call = e.request.calls[-1]
    rows = call.parameters["reply_markup"]["inline_keyboard"]
    assert any("callback_data" in button for row in rows for button in row) and rows[-1] == [{"text": "Open Memory", "url": MEMORY}]


@pytest.mark.asyncio
async def test_a_typed_question_keeps_its_input_box_and_shows_the_address_as_text(ui):
    e = ui
    await e.offer(question(input_mode="free_text", options=[]), pages=("memory",))
    e.request.enqueue("sendMessage", result=sent(e, identity=701))
    await drain(e)
    call = e.request.calls[-1]
    assert call.parameters["reply_markup"]["force_reply"] is True, "Telegram allows no link button beside an input box"
    assert call.parameters["text"].endswith("\n" + MEMORY.replace("&", "&amp;"))


@pytest.mark.asyncio
async def test_a_refused_address_on_a_question_keeps_its_choices(ui):
    e = ui
    await e.offer(question(), pages=("memory",))
    e.request.enqueue("sendMessage", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: inline keyboard button URL 'http://localhost' is invalid"})
    e.request.enqueue("sendMessage", result=sent(e, identity=701))
    await drain(e)
    await drain(e)
    call = e.request.calls[-1]
    rows = call.parameters["reply_markup"]["inline_keyboard"]
    assert all("callback_data" in button for row in rows for button in row) and call.parameters["text"].endswith("\n" + MEMORY.replace("&", "&amp;"))


async def streamed_part(e, record, output, *, plan):
    identity = await plan(Outbox(e.sessions))
    async with e.sessions() as session:
        row = await session.get(TelegramOutboxRow, identity)

    async def read(original, current_link):
        return output

    return await ModelSource(read_output=read).read_part(SendingPart(e.link, _item(row), row.parts[-1], "process", "c" * 32, e.link.token_revision))


@pytest.mark.asyncio
async def test_a_streamed_answer_gets_its_buttons_on_the_final_part(publication_environment):
    e = publication_environment
    record = replace(await _managed_turn(e), dispatch_started=True, run_id="managed-primary-run")
    output = projection(record, keys=("scheduled",), text="synthetic answer")
    ready = await streamed_part(e, record, output, plan=lambda box: box.plan_streamed_publication_reply(record, output=output, shown=shown(record), guard=e.guard_factory()))
    assert ready.page_keys == ("scheduled",)


@pytest.mark.asyncio
async def test_a_stopped_answer_has_no_inventory_to_read_and_still_sends(publication_environment):
    e = publication_environment
    record = replace(await _managed_turn(e), dispatch_started=True, run_id="managed-primary-run")
    record = await e.inbox.request_publication_stop(record, stop_id="original-stop", guard=e.guard_factory())
    output = cancelled(record)
    assert output.inventory.state != "matched" or output.inventory.document.coverage != "complete"
    await streamed_part(e, record, output, plan=lambda box: box.plan_stopped_publication_reply(record, output=output, preview=shown(record, stopped=True), guard=e.guard_factory()))


def test_a_tools_artifact_leaves_nothing_a_reply_must_carry():
    document = SimpleNamespace(
        items=(
            PageInventoryDescriptor.model_validate(page_item("memory")),
            StructuredInventoryDescriptor.model_validate(artifact_item()),
            StructuredInventoryDescriptor.model_validate(artifact_item("terminal_field")),
        )
    )
    assert [item.carrier for item in content_items(document)] == ["message_artifact", "terminal_field"]
    assert [item.carrier for item in delivery_items(document)] == ["terminal_field"]


@pytest.mark.asyncio
async def test_a_tools_artifact_does_not_stand_between_the_model_and_its_reply(publication_environment):
    e = publication_environment
    record = await _managed_turn(e)
    output = projection(record, structured=("message_artifact",))
    basis, parts = prepare_verified_reply(record, output)
    assert basis["outcome"]["projection"]["inventory"]["document"]["items"][0]["carrier"] == "message_artifact", "The saved proof keeps the whole inventory"
    assert len(parts) == 1 and parts[0]["kind"] == "text"
    with pytest.raises(ValueError):
        qualify_file_output(record, output)


def test_a_tools_artifact_beside_a_file_is_still_a_file_reply():
    record = file_record()
    path = "/mnt/user-data/outputs/report.txt"
    assert qualify_file_output(record, projection(record, file=path, structured=("message_artifact",)))


@pytest.mark.asyncio
async def test_a_result_the_model_never_read_is_still_output_the_reply_must_carry(publication_environment):
    e = publication_environment
    record = await _managed_turn(e)
    with pytest.raises(ValueError):
        prepare_verified_reply(record, projection(record, structured=("terminal_field",)))
