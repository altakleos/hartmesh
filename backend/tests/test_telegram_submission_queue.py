"""A saved typed card answer stays indivisible in the ordinary FIFO."""

from test_telegram_chat_queue import environment, message


def test_typed_submission_does_not_join_ordinary_messages_on_either_side():
    link, queue = environment()
    queue.admit(link, message(link, 1))
    queue.admit(link, message(link, 2, text="Earlier ordinary input"))
    queue.admit_submission(link, message(link, 3, text="Actual typed answer"))
    queue.admit(link, message(link, 4, text="Later ordinary input"))
    queue.admit(link, message(link, 5, text="Last ordinary input"))
    assert queue.turn_ended(link, None).text == "Earlier ordinary input"
    assert queue.turn_ended(link, None).text == "Actual typed answer"
    assert queue.current_callback(link, None) is None
    assert queue.turn_ended(link, None).text == "Later ordinary input\n\nLast ordinary input"


def test_submission_metadata_on_an_ordinary_input_grants_no_queue_boundary():
    link, queue = environment()
    queue.admit(link, message(link, 1))
    ordinary = message(link, 2)
    ordinary.metadata["human_input_response"] = {"request_id": "untrusted"}
    queue.admit(link, ordinary)
    queue.admit(link, message(link, 3))
    assert queue.turn_ended(link, None).text == "message 2\n\nmessage 3"


def test_card_callback_can_wait_behind_its_source_turn_with_same_real_origins():
    link, queue = environment()
    queue.admit(link, message(link, 1, text="Question-producing human input"))
    assert queue.admit_callback(link, message(link, 1, text="Selected new answer"), callback_id="c" * 32) == "first-waiting"
    next_input = queue.turn_ended(link, None)
    assert next_input.text == "Selected new answer" and next_input.thread_ts == "1"
    assert queue.current_callback(link, None) == "c" * 32
