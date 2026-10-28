"""Pure presentation checks; these do not establish request/source authority."""

from types import SimpleNamespace

import pytest

from app.telegram_bots.human_input import present_form, present_request
from app.telegram_bots.split import utf16_units, visible_text


def request(**changes):
    raw = {"version": 1, "kind": "human_input_request", "source": "ask_clarification", "request_id": "render-request", "question": "Choose a value", "input_mode": "free_text"}
    if changes.get("source") == "sandbox_network":
        raw.update(input_mode="single_choice", options=[{"id": value, "label": value, "value": value} for value in ("allow_temporary", "deny", "allow_sandbox")])
    return {**raw, **changes}


def text(view):
    return "".join(visible_text(part["html"]) for part in view.parts)


def buttons(view):
    return [button for row in view.keyboard for button in row]


def present(raw):
    return present_request(raw, thread_id="render-thread", reply_to_message_id=91, locale="en-US")


def test_full_choices_remain_visible_while_at_most_48_numbered_buttons_are_offered():
    options = [{"id": f"option-{i}", "label": f"Choice {i}: " + "A complete long value & <literal> " * 5, "value": f"value-{i}"} for i in range(61)]
    view = present(request(input_mode="choice_with_other", options=options))
    assert all(option["label"] in text(view) for option in options)
    assert all(utf16_units(visible_text(part["html"])) <= 4096 for part in view.parts)
    choices = [button for button in buttons(view) if button["kind"] == "clarify_option"]
    assert len(choices) == 48
    assert [button["label_values"]["label"] for button in choices] == [str(i) for i in range(1, 49)]
    assert [len(row) for row in view.keyboard[:-1]] == [8] * 6
    assert view.keyboard[-1][0]["kind"] == "clarify_other"
    assert "For a choice without a button" in text(view)
    assert not any(part["force_reply"] for part in view.parts)


def test_short_and_long_choice_labels_keep_the_complete_action_preview():
    long = "A choice whose complete wording must stay visible " * 4
    view = present(request(input_mode="choice_with_other", options=[{"id": "first", "label": "First", "value": "first-value"}, {"id": "second", "label": long, "value": "second-value"}]))
    assert long in text(view)
    assert view.keyboard[0][0]["label_values"] == {"label": "First"}
    assert view.keyboard[1][0]["label_values"]["label"] == "2. " + long[:24] + "…"
    assert view.keyboard[1][0]["payload"]["value"] == "second-value"


def test_free_text_question_has_one_reply_bar_on_its_last_complete_part():
    question = "Literal <b> & question " * 300
    view = present(request(question=question))
    assert text(view) == question
    assert len(view.parts) > 1 and not view.keyboard
    assert [part["force_reply"] for part in view.parts] == [False] * (len(view.parts) - 1) + [True]
    assert all(part["reply_to_message_id"] == 91 for part in view.parts)


@pytest.mark.parametrize("details", [None, {"host": "api.example", "port": True, "ttl_seconds": 300}, {"host": "api.example", "port": 65536, "ttl_seconds": 300}])
def test_malformed_network_details_show_generic_error_without_any_controls(details):
    view = present(request(source="sandbox_network", details=details))
    assert not view.keyboard and not any(part["force_reply"] for part in view.parts)
    assert text(view) == "Something went wrong and the message was not answered. It is kept."


def test_network_card_uses_saved_details_and_never_offers_allow_sandbox():
    view = present(request(source="sandbox_network", details={"host": "api.example&value", "port": 8443, "ttl_seconds": 420}))
    assert "api.example&value:8443" in text(view) and "7 minutes" in text(view)
    assert "&amp;value" in view.parts[0]["html"]
    assert [button["payload"]["option_id"] for button in buttons(view)] == ["allow_temporary", "deny"]
    assert all(button["payload"]["host"] == "api.example&value" and button["payload"]["port"] == 8443 for button in buttons(view))
    assert view.keyboard[0][0]["label_values"] == {"minutes": 7}


@pytest.mark.parametrize("required", [True, False])
def test_typed_form_field_has_reply_bar_or_skip_but_not_both(required):
    raw = request(version=2, input_mode="form", fields=[{"name": "value", "label": "A literal <value>", "type": "text", "required": required}])
    form = SimpleNamespace(id="a" * 32, field_index=0, revision=3, values={})
    view = present_form(raw, form, thread_id="render-thread", reply_to_message_id=91, locale="en-US")
    assert "1 of 1: A literal <value>" in text(view)
    assert bool(view.parts[-1]["force_reply"]) is required
    assert [button["kind"] for button in buttons(view)] == ([] if required else ["form_skip"])
    assert all(button["payload"]["revision"] == 3 for button in buttons(view))


def test_form_review_keeps_all_labels_and_values_with_localized_checkbox():
    raw = request(version=2, input_mode="form", fields=[{"name": "value", "label": "A literal <value>", "type": "text", "required": True}, {"name": "include", "label": "Include it", "type": "checkbox", "required": False}])
    form = SimpleNamespace(id="a" * 32, field_index=2, revision=4, values={"value": "A & B", "include": False})
    view = present_form(raw, form, thread_id="render-thread", reply_to_message_id=91, locale="en-US", view="review")
    assert "A literal <value>: A & B" in text(view) and "Include it: No" in text(view)
    assert [button["kind"] for button in buttons(view)] == ["form_send", "form_change"]
    assert not any(part["force_reply"] for part in view.parts)
