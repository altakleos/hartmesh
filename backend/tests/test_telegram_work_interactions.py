"""Work confirmation views keep complete content and an exact unsubmitted draft."""

import json
from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_actions import management_request
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.work_requests import WorkRequestReader


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "purpose,view,fields,kind",
    [("decision", "decision", {"decision": "Use <Option A>", "note": "Because <sources> agree"}, "decide_confirm"), ("review", "accept", {}, "accept_confirm"), ("review", "changes", {"note": "Keep every <detail>"}, "changes_confirm")],
)
@pytest.mark.parametrize("locale", ["en", "zh-CN"])
async def test_confirmation_keeps_complete_preview_and_exact_seen(work_pages, purpose, view, fields, kind, locale):
    from app.telegram_bots.work_interactions import present_interaction
    from deerflow.agent_instances.human_input_contract import Respond

    e = work_pages
    record, request, _seen = await management_request(e, purpose)
    for number in range(4):
        await e.human.respond(actor=e.actor, request_id=request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, text=f"Comment {number}: " + "Source <detail> " * 200))
    source = await WorkRequestReader(work=e.human.work, authority=e.live.authority, links=e.db.links).read_request(e.link, request["id"])
    state = {"view": view, **fields}
    shown = present_interaction(source, state, locale=locale)
    assert "".join(part["visible_text"] for part in shown.parts).count("Source <detail>") == 800
    text = "\n".join(part["visible_text"] for part in shown.parts)
    assert len(shown.parts) > 1 and "Comment 0:" in text and "Comment 3:" in text
    assert request["question"] in text and request["reason"] in text
    confirm = next(button for line in shown.keyboard for button in line if button["kind"] == kind)
    assert confirm["payload"]["seen"] == source.seen
    assert confirm["payload"]["interaction"] == state
    assert "acknowledge_unchecked_sources" not in str(shown)
    assert present_interaction(source, json.loads(json.dumps(state)), locale=locale) == shown
    assert (await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=record["id"]))["revision"] == source.seen["work_revision"]


@pytest.mark.asyncio
async def test_cannot_choices_and_prompt_save_nothing_and_keep_original_basis(work_pages):
    from app.telegram_bots.work_interactions import present_interaction

    e = work_pages
    source = await WorkRequestReader(work=e.human.work, authority=e.live.authority, links=e.db.links).read_request(e.link, e.request["id"])
    menu = present_interaction(source, {"view": "cannot"}, locale="en")
    assert [[button["kind"] for button in line] for line in menu.keyboard] == [["hi_cannot"], ["hi_wrong"], ["back"]]
    prompt = present_interaction(source, {"view": "prompt", "purpose": "cannot"}, locale="en")
    assert not prompt.keyboard
    assert "In a few words, why?" in prompt.parts[-1]["visible_text"]
    assert (await e.human.get(actor=e.actor, request_id=e.request["id"]))["revision"] == source.seen["revision"]


@pytest.mark.asyncio
@pytest.mark.parametrize("view,kinds", [("answer", {"answer_yes", "answer_no"}), ("voice", {"voice_save_answer", "voice_discard"})])
async def test_unsubmitted_text_or_transcript_requires_explicit_choice(work_pages, view, kinds):
    from app.telegram_bots.work_interactions import present_interaction

    e = work_pages
    source = await WorkRequestReader(work=e.human.work, authority=e.live.authority, links=e.db.links).read_request(e.link, e.request["id"])
    shown = present_interaction(source, {"view": view, "text": "An <unsubmitted> answer"}, locale="en")
    assert {button["kind"] for line in shown.keyboard for button in line} == kinds
    assert "An <unsubmitted> answer" in "\n".join(part["visible_text"] for part in shown.parts)
    assert all(button["payload"]["interaction"]["text"] == "An <unsubmitted> answer" for line in shown.keyboard for button in line)
    assert not (await e.human.responses(actor=e.actor, request_id=e.request["id"]))["responses"]


@pytest.mark.asyncio
async def test_stale_changes_note_is_retained_only_as_a_fresh_confirmation(work_pages):
    from app.telegram_bots.work_interactions import present_interaction
    from deerflow.agent_instances.human_input_contract import Respond

    e = work_pages
    record, request, _seen = await management_request(e, "review")
    reader = WorkRequestReader(work=e.human.work, authority=e.live.authority, links=e.db.links)
    old = await reader.read_request(e.link, request["id"])
    state = {"view": "changes", "note": "Keep this note across restart"}
    previous = present_interaction(old, state, locale="en")
    await e.human.respond(actor=e.actor, request_id=request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, text="A new review comment"))
    current = await reader.read_request(e.link, request["id"])
    refreshed = present_interaction(current, json.loads(json.dumps(state)), locale="en")
    assert previous.keyboard[0][0]["payload"]["seen"] == old.seen
    assert refreshed.keyboard[0][0]["payload"]["seen"] == current.seen != old.seen
    assert refreshed.keyboard[0][0]["payload"]["interaction"] == state
    assert "A new review comment" in "\n".join(part["visible_text"] for part in refreshed.parts)
    assert {button["kind"] for line in refreshed.keyboard for button in line} == {"changes_confirm", "changes_open", "dismiss"}
    assert (await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=record["id"]))["status"] == "submitted"


@pytest.mark.asyncio
async def test_interaction_rejects_wrong_purpose_permissions_and_unshown_fields(work_pages):
    from copy import deepcopy
    from dataclasses import replace

    from app.telegram_bots.work_interactions import present_interaction
    from deerflow.agent_instances.contract import AgentDenied

    e = work_pages
    source = await WorkRequestReader(work=e.human.work, authority=e.live.authority, links=e.db.links).read_request(e.link, e.request["id"])
    for state in ({"view": "accept"}, {"view": "answer", "text": "Answer", "acknowledge_unchecked_sources": True}, {"view": "unknown"}, {"view": "prompt", "purpose": "decision"}, {"view": "answer", "text": "x" * 4097}):
        with pytest.raises(ValueError):
            present_interaction(source, state, locale="en")
    denied = replace(source, request={**source.request, "can_respond": False})
    with pytest.raises(AgentDenied):
        present_interaction(denied, {"view": "answer", "text": "No access"}, locale="en")
    state = {"view": "answer", "text": "Allowed"}
    original = deepcopy(source.seen)
    shown = present_interaction(source, state, locale="en")
    shown.keyboard[0][0]["payload"]["seen"]["response_ids"].append("foreign")
    assert source.seen == original
    assert shown.keyboard[1][0]["payload"]["seen"] == original


@pytest.mark.asyncio
@pytest.mark.parametrize("locale", ["en", "zh-CN"])
async def test_decision_note_prompt_shows_exact_unsubmitted_selection(work_pages, locale):
    from app.telegram_bots.work_interactions import present_interaction

    e = work_pages
    _record, request, _seen = await management_request(e, "decision")
    source = await WorkRequestReader(work=e.human.work, authority=e.live.authority, links=e.db.links).read_request(e.link, request["id"])
    shown = present_interaction(source, {"view": "prompt", "purpose": "decision_note", "decision": "  Choose <A> & keep details  "}, locale=locale)
    text = "\n".join(part["visible_text"] for part in shown.parts)
    assert "Choose <A> & keep details" in text
    assert "Choose &lt;A&gt; &amp; keep details" in "".join(part["html"] for part in shown.parts)
    assert not shown.keyboard
    assert (await e.human.get(actor=e.actor, request_id=request["id"]))["revision"] == source.seen["revision"]
