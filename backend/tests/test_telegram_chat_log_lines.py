"""A chat that stops says whose it is and why, in a line an operator can search for."""

import asyncio
import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.telegram_bots import turns as turns_module
from app.telegram_bots.adapter import TelegramBotChannel
from app.telegram_bots.producer import OutboxSender
from app.telegram_bots.progress import _final_read
from app.telegram_bots.turns import TURN_WAIT_REPORT_SECONDS, ChatTurns, _Turn

LINK = SimpleNamespace(id="a" * 32, bot_id=9_000_000_001)
RECORD = SimpleNamespace(publication_id="b" * 32, run_id=None)  # A live turn's record never learns its run.
PRIVATE = "123456:private-token person@example.com"
HERE = "test_telegram_chat_log_lines.py"
CHAT = f"link={LINK.id} bot={LINK.bot_id} turn={RECORD.publication_id} run=run-1"


def chat_turn(*, record=RECORD, run_id="run-1", clock=lambda: 0.0):
    return _Turn(SimpleNamespace(link=LINK, _time=clock), LINK, None, (SimpleNamespace(message_thread_id=None),), record=record, run_id=run_id)


def lines(caplog):
    return [record.getMessage() for record in caplog.records if record.getMessage().startswith("telegram_bots ")]


def raised(error):
    try:
        raise error
    except Exception as caught:
        return caught


def test_a_line_names_the_chat_the_turn_the_run_and_the_cause(caplog):
    chat_turn().held("chat_observation_held", raised(ValueError(PRIVATE)))
    assert lines(caplog)[0].startswith(f"telegram_bots chat_observation_held {CHAT} cause=ValueError:{HERE}:")
    assert PRIVATE not in caplog.text


def test_a_line_leaves_out_what_a_turn_does_not_have_yet(caplog):
    chat_turn(record=None, run_id=None).held("chat_preview_held")
    assert lines(caplog) == [f"telegram_bots chat_preview_held link={LINK.id} bot={LINK.bot_id}"]


@pytest.mark.asyncio
async def test_a_failed_task_names_its_chat_with_or_without_a_turn(caplog):
    turns = ChatTurns(SimpleNamespace(config=SimpleNamespace(queue=SimpleNamespace(depth=10))))

    async def fail():
        raise ValueError("fixed-word")

    channel = SimpleNamespace(link=LINK)
    await asyncio.gather(turns._spawn(fail(), channel=channel, turn=chat_turn()), turns._spawn(fail(), channel=channel), return_exceptions=True)
    await asyncio.sleep(0)
    first, second = lines(caplog)
    assert first.startswith(f"telegram_bots chat_turn_held {CHAT} cause=ValueError:fixed-word:{HERE}:")
    assert second.startswith(f"telegram_bots chat_turn_held link={LINK.id} bot={LINK.bot_id} cause=ValueError:fixed-word:{HERE}:")


def test_a_turn_that_keeps_waiting_says_so_once(caplog):
    now = [100.0]
    turn = chat_turn(clock=lambda: now[0])
    run = SimpleNamespace(row={"status": "running"})

    ChatTurns._report_wait(turn, run)
    now[0] += TURN_WAIT_REPORT_SECONDS - 1
    ChatTurns._report_wait(turn, run)
    assert lines(caplog) == []

    now[0] += 1
    ChatTurns._report_wait(turn, run)
    now[0] += TURN_WAIT_REPORT_SECONDS
    ChatTurns._report_wait(turn, run)
    assert lines(caplog) == [f"telegram_bots chat_turn_waiting {CHAT} status=running waited_s={TURN_WAIT_REPORT_SECONDS}"]


def test_a_turn_with_no_run_to_read_still_says_it_is_waiting(caplog):
    now = [0.0]
    turn = chat_turn(clock=lambda: now[0])
    ChatTurns._report_wait(turn, None)
    now[0] += TURN_WAIT_REPORT_SECONDS
    ChatTurns._report_wait(turn, None)
    assert lines(caplog) == [f"telegram_bots chat_turn_waiting {CHAT} waited_s={TURN_WAIT_REPORT_SECONDS}"]


def test_every_line_of_the_turn_controller_goes_through_the_one_that_names_the_chat():
    source = Path(turns_module.__file__).read_text(encoding="utf-8")
    assert len(re.findall(r"\blogger\.", source)) == 1


def test_why_a_finished_run_cannot_be_delivered_is_closed_words():
    assert _final_read(None) == {"output": "absent"}

    def read(document, *, inventory="matched", reason=None):
        return SimpleNamespace(output=SimpleNamespace(state="unavailable", reason="unsupported_output"), inventory=SimpleNamespace(state=inventory, reason=reason, document=document))

    incomplete = _final_read(read(SimpleNamespace(coverage="incomplete", reason="scan_incomplete")))
    assert incomplete == {"output": "unavailable:unsupported_output", "inventory": "matched:incomplete:scan_incomplete"}
    assert _final_read(read(None, inventory="unavailable", reason="source_missing"))["inventory"] == "unavailable:None:source_missing"


@pytest.mark.asyncio
async def test_a_failed_handler_names_its_cause_and_not_its_text(caplog):
    await TelegramBotChannel._on_error(SimpleNamespace(link=LINK), None, SimpleNamespace(error=raised(ValueError(PRIVATE))))
    assert lines(caplog)[0].startswith(f"telegram_bots handler_failed link={LINK.id} bot={LINK.bot_id} cause=ValueError:{HERE}:")
    assert PRIVATE not in caplog.text


@pytest.mark.asyncio
async def test_a_failed_delivery_worker_is_logged_and_still_released(caplog):
    released = asyncio.Event()

    async def release(*_args):
        released.set()

    async def fail():
        raise ValueError(PRIVATE)

    sender = SimpleNamespace(_callbacks=set(), _release_tasks={}, _release=release, _release_done=lambda _claim, _task: None)
    task = asyncio.create_task(fail())
    await asyncio.gather(task, return_exceptions=True)
    OutboxSender._worker_done(sender, LINK.id, "row-1", 1, "claim-1", task)
    await asyncio.wait_for(released.wait(), 3)
    assert lines(caplog)[0].startswith(f"telegram_bots sender_worker_failed link={LINK.id} delivery=row-1 cause=ValueError:{HERE}:")
    assert PRIVATE not in caplog.text
