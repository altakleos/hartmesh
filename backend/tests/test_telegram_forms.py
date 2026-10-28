"""Durable form progress and exact browser-compatible submission values."""

import importlib
from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import text
from test_telegram_callbacks import callbacks_environment as callbacks_environment
from test_telegram_publication_custody import publication_environment as publication_environment

from deerflow.persistence.telegram_bots.model import TelegramCallbackRow


def api():
    return importlib.import_module("app.telegram_bots.forms")


def request(fields=None):
    return {
        "version": 2,
        "kind": "human_input_request",
        "source": "ask_clarification",
        "request_id": "actual-request",
        "question": "Details, please",
        "input_mode": "form",
        "fields": fields
        or [
            {"name": "company", "label": "Company", "type": "text", "required": True},
            {"name": "copy", "label": "Send a copy?", "type": "checkbox", "required": False},
        ],
    }


@pytest.mark.parametrize(
    "values,expected",
    [
        ({"company": "Jones Ltd"}, 'Company: Jones Ltd; Send a copy?: no [values: {"company":"Jones Ltd","copy":false}]'),
        ({"company": " Jones Ltd ", "copy": True}, 'Company:  Jones Ltd ; Send a copy?: yes [values: {"company":" Jones Ltd ","copy":true}]'),
        ({"company": "   ", "copy": False}, 'Send a copy?: no [values: {"copy":false}]'),
    ],
)
def test_submission_uses_real_web_checkbox_and_whitespace_semantics(values, expected):
    assert api().form_submission_value(request()["fields"], values) == expected


def test_submission_preserves_array_values_and_javascript_integer_key_order():
    fields = [
        {"name": "10", "label": "Ten", "type": "text", "required": False},
        {"name": "2", "label": "Two", "type": "text", "required": False},
        {"name": "sites", "label": "Sites", "type": "multi_select", "required": False, "options": [{"id": "a", "label": "A", "value": "A"}, {"id": "b", "label": "B", "value": "B"}]},
    ]
    assert api().form_submission_value(fields, {"10": "ten", "2": "two", "sites": ["B", "A"]}) == 'Ten: ten; Two: two; Sites: B, A [values: {"2":"two","10":"ten","sites":["B","A"]}]'


@pytest.mark.asyncio
async def test_form_progress_replays_exact_gesture_and_required_field_cannot_skip(callbacks_environment):
    e = callbacks_environment
    forms = api().Forms(e.callbacks)
    form = await forms.create(e.link, form_id=uuid4().hex, thread_id=e.thread_id, request=request(), guard=e.guard_factory())
    with pytest.raises(ValueError):
        await forms.transition(e.link, form.id, expected_revision=0, update_id=100, action="skip", field="company", guard=e.guard_factory())
    first = await forms.transition(e.link, form.id, expected_revision=0, update_id=101, action="answer", field="company", value="Jones Ltd", guard=e.guard_factory())
    replay = await forms.transition(e.link, form.id, expected_revision=0, update_id=101, action="answer", field="company", value="Jones Ltd", guard=e.guard_factory())
    assert replay == first and first.field_index == 1 and first.revision == 1
    with pytest.raises(ValueError):
        await forms.transition(e.link, form.id, expected_revision=0, update_id=101, action="answer", field="company", value="Changed", guard=e.guard_factory())
    review = await forms.transition(e.link, form.id, expected_revision=1, update_id=102, action="skip", field="copy", guard=e.guard_factory())
    assert review.state == "review" and dict(review.values) == {"company": "Jones Ltd", "copy": False}
    with pytest.raises(TypeError):
        review.values["company"] = "forged"
    reopened = api().Forms(e.callbacks)
    assert await reopened.read(e.link, form.id, guard=e.guard_factory()) == review


@pytest.mark.asyncio
async def test_multiselect_toggle_replay_is_once_and_old_revision_cannot_advance(callbacks_environment):
    e = callbacks_environment
    forms = api().Forms(e.callbacks)
    fields = [{"name": "sites", "label": "Sites", "type": "multi_select", "required": True, "options": [{"id": "a", "label": "A", "value": "alpha"}, {"id": "b", "label": "B", "value": "beta"}]}]
    form = await forms.create(e.link, form_id=uuid4().hex, thread_id=e.thread_id, request=request(fields), guard=e.guard_factory())
    with pytest.raises(ValueError):
        await forms.transition(e.link, form.id, expected_revision=0, update_id=110, action="next", field="sites", guard=e.guard_factory())
    selected = await forms.transition(e.link, form.id, expected_revision=0, update_id=111, action="toggle", field="sites", value="a", guard=e.guard_factory())
    assert dict(selected.values) == {"sites": ("alpha",)}
    assert await forms.transition(e.link, form.id, expected_revision=0, update_id=111, action="toggle", field="sites", value="a", guard=e.guard_factory()) == selected
    with pytest.raises(ValueError):
        await forms.transition(e.link, form.id, expected_revision=0, update_id=112, action="next", field="sites", guard=e.guard_factory())
    review = await forms.transition(e.link, form.id, expected_revision=1, update_id=113, action="next", field="sites", guard=e.guard_factory())
    assert review.state == "review"


@pytest.mark.asyncio
async def test_form_borrowed_changes_rollback_with_its_required_ui_plan(callbacks_environment):
    e = callbacks_environment
    forms = api().Forms(e.callbacks)
    identity = uuid4().hex
    with pytest.raises(RuntimeError, match="required UI failed"):
        async with e.sessions() as writer, writer.begin():
            if e.db.dialect == "sqlite":
                await writer.execute(text("BEGIN IMMEDIATE"))
            await forms.create(e.link, form_id=identity, thread_id=e.thread_id, request=request(), guard=e.guard_factory(), session=writer)
            raise RuntimeError("required UI failed")
    async with e.sessions() as reader:
        assert await reader.get(TelegramCallbackRow, identity) is None


@pytest.mark.asyncio
async def test_retired_original_guard_rolls_back_form_transition(callbacks_environment):
    e = callbacks_environment
    forms = api().Forms(e.callbacks)
    form = await forms.create(e.link, form_id=uuid4().hex, thread_id=e.thread_id, request=request(), guard=e.guard_factory())
    guard = e.guard_factory()
    original = guard.check
    calls = [0]

    async def retire_last(writer):
        calls[0] += 1
        if calls[0] == 2:
            e.attempt_current[0] = False
        return await original(writer)

    from types import SimpleNamespace

    guard = SimpleNamespace(captured_link=guard.captured_link, check=retire_last)
    with pytest.raises(ValueError):
        await forms.transition(e.link, form.id, expected_revision=0, update_id=114, action="answer", field="company", value="Jones Ltd", guard=guard)
    e.attempt_current[0] = True
    assert (await forms.read(e.link, form.id, guard=e.guard_factory())).revision == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("corrupt", [False, True])
async def test_form_send_uses_actual_frozen_control_and_does_not_submit_before_owner_is_saved(callbacks_environment, corrupt):
    e = callbacks_environment
    forms = api().Forms(e.callbacks)
    fields = [{"name": "copy", "label": "Send a copy?", "type": "checkbox", "required": False}]

    async def review(identity, request_id, update_id):
        offered = {**request(fields), "request_id": request_id}
        form = await forms.create(e.link, form_id=identity, thread_id=e.thread_id, request=offered, guard=e.guard_factory())
        return await forms.transition(e.link, identity, expected_revision=form.revision, update_id=update_id, action="skip", field="copy", guard=e.guard_factory())

    first = await review(uuid4().hex, "first-request", 201)
    second = await review(uuid4().hex, "second-request", 202)
    plan = await e.plan(keyboard=[[{"label": "label.try_again", "kind": "form_send", "target_kind": "thread", "target_id": e.thread_id, "payload": {"form_id": first.id, "revision": first.revision}}]], message_id=902)
    identity = plan.group.callback_ids[0]
    if corrupt:
        async with e.sessions.begin() as writer:
            selected = await writer.get(TelegramCallbackRow, identity)
            selected.payload = {**selected.payload, "form_id": second.id}
    async with e.callbacks.writer(e.link, e.guard_factory()) as writer:
        if corrupt:
            with pytest.raises(ValueError):
                await forms.submission(e.link, identity, guard=e.guard_factory(), session=writer)
        else:
            answer = await forms.submission(e.link, identity, guard=e.guard_factory(), session=writer)
            assert answer.form.id == first.id
            assert answer.value == 'Send a copy?: no [values: {"copy":false}]'
            assert dict(answer.response) == {"version": 1, "kind": "human_input_response", "source": "ask_clarification", "request_id": "first-request", "response_kind": "text", "value": answer.value}
    assert (await forms.read(e.link, first.id, guard=e.guard_factory())).state == "review"


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["answer", "submission"])
@pytest.mark.parametrize("checked", [False, True])
async def test_required_checkbox_needs_true_at_answer_and_at_submission(callbacks_environment, phase, checked):
    e = callbacks_environment
    forms = api().Forms(e.callbacks)
    fields = [{"name": "agree", "label": "I agree", "type": "checkbox", "required": True}]
    form = await forms.create(e.link, form_id=uuid4().hex, thread_id=e.thread_id, request=request(fields), guard=e.guard_factory())
    if phase == "answer":
        if checked:
            result = await forms.transition(e.link, form.id, expected_revision=0, update_id=301, action="answer", field="agree", value=True, guard=e.guard_factory())
            assert result.state == "review" and result.values["agree"] is True
        else:
            with pytest.raises(ValueError):
                await forms.transition(e.link, form.id, expected_revision=0, update_id=301, action="answer", field="agree", value=False, guard=e.guard_factory())
            assert (await forms.read(e.link, form.id, guard=e.guard_factory())).revision == 0
        return
    form = await forms.transition(e.link, form.id, expected_revision=0, update_id=302, action="answer", field="agree", value=True, guard=e.guard_factory())
    plan = await e.plan(keyboard=[[{"label": "label.try_again", "kind": "form_send", "target_kind": "thread", "target_id": e.thread_id, "payload": {"form_id": form.id, "revision": form.revision}}]], message_id=902)
    if not checked:
        # The Send reader must reject an invalid saved review independently of
        # the earlier UI transition; no repaired source or new request is used.
        async with e.sessions.begin() as writer:
            saved = await writer.get(TelegramCallbackRow, form.id)
            saved.payload = {**saved.payload, "values": {"agree": False}}
    async with e.callbacks.writer(e.link, e.guard_factory()) as writer:
        if checked:
            submitted = await forms.submission(e.link, plan.group.callback_ids[0], guard=e.guard_factory(), session=writer)
            assert submitted.response["value"] == 'I agree: yes [values: {"agree":true}]'
        else:
            with pytest.raises(ValueError):
                await forms.submission(e.link, plan.group.callback_ids[0], guard=e.guard_factory(), session=writer)


@pytest.mark.asyncio
async def test_earlier_queued_plain_message_can_end_newer_form_without_reopening_it(callbacks_environment):
    e = callbacks_environment
    forms = api().Forms(e.callbacks)
    queued_update = e.origins[-1].update_id  # Actual retained input before the form's later gestures.
    form = await forms.create(e.link, form_id=uuid4().hex, thread_id=e.thread_id, request=request(), guard=e.guard_factory())
    form = await forms.transition(e.link, form.id, expected_revision=0, update_id=queued_update + 10, action="answer", field="company", value="Jones Ltd", guard=e.guard_factory())
    before = form.revision
    cancelled = await forms.transition(e.link, form.id, expected_revision=before, update_id=queued_update, action="cancel", guard=e.guard_factory())
    assert cancelled.state == "cancelled" and cancelled.last_update_id == queued_update and cancelled.revision == before + 1
    assert await forms.transition(e.link, form.id, expected_revision=before, update_id=queued_update, action="cancel", guard=e.guard_factory()) == cancelled
    with pytest.raises(ValueError):
        await forms.transition(e.link, form.id, expected_revision=cancelled.revision, update_id=queued_update + 20, action="answer", field="copy", value=True, guard=e.guard_factory())
    assert (await forms.read(e.link, form.id, guard=e.guard_factory())).state == "cancelled"
