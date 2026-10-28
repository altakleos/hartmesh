"""Pure waiting order; publication, permissions and SQL belong to the controller."""

import importlib
from dataclasses import replace
from types import SimpleNamespace

import pytest

from app.channels.message_bus import InboundMessage
from app.telegram_bots.config import TelegramBotsConfig


def environment(*, depth=10):
    link = SimpleNamespace(id="a" * 32, owner_user_id="human", bot_id=9001, agent_kind="lead_agent", agent_ref="lead_agent", connection_id="connection", telegram_user_id=7001, chat_id=7001, binding_generation=1, token_revision=1)
    queue = importlib.import_module("app.telegram_bots.queue").ChatQueue(TelegramBotsConfig(queue={"depth": depth}))
    return link, queue


def message(link, identity, *, text=None, topic=None, files=()):
    return InboundMessage(
        "telegram",
        str(link.chat_id),
        str(link.telegram_user_id),
        text or f"message {identity}",
        thread_ts=str(identity),
        topic_id=str(topic) if topic is not None else None,
        connection_id=link.connection_id,
        owner_user_id=link.owner_user_id,
        workspace_id=str(link.bot_id),
        files=list(files),
        metadata={"message_id": identity, "message_thread_id": topic},
    )


def test_followups_wait_without_a_conversation_or_manager_worker():
    link, queue = environment()
    assert queue.admit(link, message(link, 1)) == "run"
    assert queue.admit(link, message(link, 2)) == "first-waiting"
    assert queue.admit(link, message(link, 3)) == "waiting"
    assert queue.waiting_count(link, None) == 2


def test_followups_join_in_order_with_first_reply_target_and_complete_files():
    link, queue = environment()
    first, second = message(link, 2, text="Full second input", topic=99, files=({"name": "second.txt"},)), message(link, 3, text="Full third input", topic=99, files=({"name": "third.txt"},))
    queue.admit(link, message(link, 1, topic=99))
    queue.admit(link, first)
    queue.admit(link, second)
    joined = queue.turn_ended(link, 99)
    assert joined.text == "Full second input\n\nFull third input"
    assert joined.thread_ts == "2" and joined.topic_id == "99"
    assert joined.files == [{"name": "second.txt"}, {"name": "third.txt"}]
    assert joined.metadata["telegram_source_message_ids"] == [2, 3]
    assert first.text == "Full second input" and first.files == [{"name": "second.txt"}]
    assert "telegram_source_message_ids" not in first.metadata
    assert queue.admit(link, message(link, 4, topic=99)) == "first-waiting"


def test_eleventh_waiting_message_is_refused_and_never_joined():
    link, queue = environment()
    queue.admit(link, message(link, 1))
    for identity in range(2, 12):
        assert queue.admit(link, message(link, identity)) == ("first-waiting" if identity == 2 else "waiting")
    assert queue.admit(link, message(link, 12, text="refused input")) == "full"
    assert queue.waiting_count(link, None) == 10
    joined = queue.turn_ended(link, None)
    assert joined.metadata["telegram_source_message_ids"] == list(range(2, 12))
    assert "refused input" not in joined.text


def test_retry_hold_keeps_the_original_turn_open_until_its_outcome():
    link, queue = environment()
    initial = message(link, 1)
    queue.admit(link, initial)
    queue.retry_later(link, initial)
    assert queue.admit(link, message(link, 2)) == "first-waiting"
    assert queue.admit(link, message(link, 3)) == "waiting"
    assert queue.turn_ended(link, None).text == "message 2\n\nmessage 3"


def test_stop_or_cancel_drops_followups_before_the_turn_is_released():
    link, queue = environment()
    initial = message(link, 1)
    queue.admit(link, initial)
    queue.retry_later(link, initial)
    queue.admit(link, message(link, 2))
    assert queue.cancel_wait(link, None) is True
    with pytest.raises(ValueError):
        queue.retry_later(link, initial)
    assert queue.turn_ended(link, None) is None
    assert queue.cancel_wait(link, None) is False
    assert queue.admit(link, message(link, 3)) == "run"


def test_topics_general_normalization_and_other_links_have_separate_waiters():
    link, queue = environment()
    other = SimpleNamespace(**{**vars(link), "id": "b" * 32})
    assert queue.admit(link, message(link, 1, topic=1)) == "run"
    assert queue.admit(link, message(link, 2)) == "first-waiting"
    assert queue.admit(link, message(link, 3, topic=99)) == "run"
    assert queue.admit(other, message(other, 1)) == "run"
    assert queue.turn_ended(link, 99) is None
    assert queue.turn_ended(other, None) is None
    assert queue.turn_ended(link, None).thread_ts == "2"


@pytest.mark.parametrize("field,value", [("binding_generation", 2), ("token_revision", 2), ("owner_user_id", "other"), ("telegram_user_id", 7002)])
def test_changed_scope_cannot_release_or_append_to_an_existing_turn(field, value):
    link, queue = environment()
    queue.admit(link, message(link, 1))
    queue.admit(link, message(link, 2))
    changed = SimpleNamespace(**{**vars(link), field: value})
    assert queue.turn_ended(changed, None) is None
    assert queue.waiting_count(changed, None) == 0
    with pytest.raises(ValueError):
        queue.admit(changed, message(changed, 3))
    assert queue.turn_ended(link, None).thread_ts == "2"


@pytest.mark.parametrize("topic", [True, 1.0, 0, -1, "01"])
def test_ambiguous_topic_types_never_enter_the_queue(topic):
    link, queue = environment()
    incoming = message(link, 1)
    incoming.metadata["message_thread_id"] = topic
    with pytest.raises(ValueError):
        queue.admit(link, incoming)


def test_close_returns_owned_inputs_without_publishing_and_rejects_new_admission():
    link, queue = environment()
    queue.admit(link, message(link, 1))
    waiting = message(link, 2, files=({"name": "whole.txt"},))
    queue.admit(link, waiting)
    waiting.files[0]["name"] = "mutated outside the queue"
    held = queue.close()
    assert [item.thread_ts for item in held] == ["1", "2"]
    assert held[1].files == [{"name": "whole.txt"}]
    assert queue.close() == ()
    with pytest.raises(ValueError):
        queue.admit(link, message(link, 3))


def test_mismatched_destination_and_prebound_execution_are_refused():
    link, queue = environment()
    incoming = message(link, 1)
    with pytest.raises(ValueError):
        queue.admit(link, replace(incoming, chat_id="other"))
    with pytest.raises(ValueError):
        queue.admit(link, replace(incoming, execution_required=True))


def test_duplicate_input_does_not_take_another_waiting_position():
    link, queue = environment()
    initial, waiting = message(link, 1), message(link, 2)
    queue.admit(link, initial)
    queue.admit(link, waiting)
    for repeated in (initial, waiting):
        with pytest.raises(ValueError):
            queue.admit(link, repeated)
    assert queue.waiting_count(link, None) == 1
    assert queue.turn_ended(link, None).metadata["telegram_source_message_ids"] == [2]


def test_conflicting_topic_carriers_and_boolean_lookup_do_not_release_a_turn():
    link, queue = environment()
    queue.admit(link, message(link, 1))
    queue.admit(link, message(link, 2))
    with pytest.raises(ValueError):
        queue.admit(link, replace(message(link, 3, topic=99), topic_id="7"))
    assert queue.turn_ended(link, True) is None
    assert queue.waiting_count(link, None) == 1
    assert queue.turn_ended(link, 1).thread_ts == "2"


def test_callback_input_is_an_indivisible_fifo_unit_between_ordinary_prefixes():
    link, queue = environment()
    callback = "c" * 32
    queue.admit(link, message(link, 1))
    queue.admit(link, message(link, 2, text="Ordinary before callback"))
    queue.admit_callback(link, message(link, 3, text="Kept callback input", files=({"name": "kept.txt"},)), callback_id=callback)
    queue.admit(link, message(link, 4, text="Ordinary after callback"))
    queue.admit(link, message(link, 5, text="Last ordinary"))
    assert queue.turn_ended(link, None).text == "Ordinary before callback"
    assert queue.current_callback(link, None) is None
    captured = queue.waiting_inputs(link, None)
    assert [(item.inbound.thread_ts, item.callback_id) for item in captured] == [("3", callback), ("4", None), ("5", None)]
    captured[0].inbound.files[0]["name"] = "outside mutation"
    joined = queue.turn_ended(link, None)
    assert joined.text == "Kept callback input" and joined.files == [{"name": "kept.txt"}]
    assert queue.current_callback(link, None) == callback
    assert queue.turn_ended(link, None).text == "Ordinary after callback\n\nLast ordinary"
    assert queue.current_callback(link, None) is None


def test_consecutive_callback_inputs_remain_separate_and_provider_metadata_grants_nothing():
    link, queue = environment()
    incoming = message(link, 1)
    incoming.metadata["callback_id"] = "e" * 32
    queue.admit(link, incoming)
    assert queue.current_callback(link, None) is None
    queue.admit_callback(link, message(link, 2), callback_id="c" * 32)
    queue.admit_callback(link, message(link, 3), callback_id="d" * 32)
    assert queue.turn_ended(link, None).thread_ts == "2"
    assert queue.current_callback(link, None) == "c" * 32
    assert queue.turn_ended(link, None).thread_ts == "3"
    assert queue.current_callback(link, None) == "d" * 32
    assert queue.turn_ended(link, None) is None


def test_invalid_or_repeated_callback_input_never_takes_a_waiting_position():
    link, queue = environment()
    queue.admit(link, message(link, 1))
    for value in [True, "invalid", "C" * 32]:
        with pytest.raises(ValueError):
            queue.admit_callback(link, message(link, 2), callback_id=value)
    queue.admit_callback(link, message(link, 2), callback_id="c" * 32)
    with pytest.raises(ValueError):
        queue.admit_callback(link, message(link, 2), callback_id="d" * 32)
    assert queue.waiting_count(link, None) == 1
    queue.cancel_wait(link, None)
    assert queue.waiting_inputs(link, None) == ()
    assert queue.turn_ended(link, None) is None


def test_input_after_stop_is_a_new_waiter_without_resurrecting_the_dropped_tail():
    link, queue = environment()
    queue.admit(link, message(link, 1))
    queue.admit(link, message(link, 2))
    queue.cancel_wait(link, None)
    assert queue.admit(link, message(link, 3)) == "first-waiting"
    assert queue.turn_ended(link, None).metadata["telegram_source_message_ids"] == [3]
