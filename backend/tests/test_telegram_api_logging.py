"""API failure events retain structured facts without remote descriptions."""

import logging

import pytest
from telegram.error import BadRequest, Conflict, Forbidden, InvalidToken, NetworkError, RetryAfter, TimedOut

from app.telegram_bots.api_logging import call_failure_fields, log_call_failure


@pytest.mark.parametrize(
    "error,http,desc",
    [
        (Conflict("terminated by other getUpdates"), 409, "other-poller"),
        (Conflict("webhook is set"), 409, "webhook"),
        (Forbidden("blocked by the user"), 403, "blocked"),
        (Forbidden("can't initiate conversation"), 403, "not-started"),
        (BadRequest("can't parse entities: private content"), 400, "parse-entities"),
        (BadRequest("message is not modified"), 400, "not-modified"),
        (BadRequest("message can't be edited"), 400, "edit-anchor-missing"),
        (BadRequest("query is too old"), 400, "query-too-old"),
        (BadRequest("message thread not found"), 400, "thread-not-found"),
        (BadRequest("URL is INVALID"), 400, "button-url"),
        (BadRequest("file is too big"), 400, "too-big"),
        (BadRequest("unlisted private text"), 400, "other"),
        (InvalidToken("Unauthorized"), 401, None),
        (InvalidToken("Not Found"), 404, None),
        (NetworkError("private request address"), None, None),
        (TimedOut(), None, None),
        (RetryAfter(7), 429, None),
    ],
)
def test_error_classification_uses_only_known_status_and_closed_descriptions(error, http, desc):
    fields = call_failure_fields(error)
    expected = {"error": type(error).__name__}
    if http is not None:
        expected["http"] = http
    if desc is not None:
        expected["desc"] = desc
    assert fields == expected


def test_cause_and_wrapped_invalid_token_are_classified_without_their_text():
    original = InvalidToken("Unauthorized")
    wrapped = InvalidToken("private credential was rejected")
    wrapped.__cause__ = original
    assert call_failure_fields(wrapped) == {"error": "InvalidToken", "cause": "InvalidToken", "http": 401}


def test_failure_record_has_one_owner_and_complete_claim_facts(caplog):
    error = BadRequest("unlisted private text")
    with caplog.at_level(logging.WARNING, logger="app.telegram_bots.supervisor"):
        log_call_failure(link_id="link", bot_id=1234567, method="sendMessage", error=error, attempts=2, outbox_id="delivery", part_id="part", class_="reply", claim_owner="process", claim_id="claim", failed_reason="invalid-payload")
    assert len(caplog.records) == 1
    message = caplog.records[0].getMessage()
    assert message.startswith("telegram_bots send_failed link=link bot=1234567 method=sendMessage error=BadRequest ")
    assert "private" not in message and "attempts=2" in message and "desc=other" in message
    assert message.index("attempts=") < message.index("desc=")
    assert "outbox_id=delivery" in message and "claim_id=claim" in message and "failed_reason=invalid-payload" in message
    assert error._hartmesh_telegram_failure_logged is True


@pytest.mark.parametrize("explicit", [False, True])
def test_native_and_bounded_long_rate_refusals_use_retry_after_event(caplog, explicit):
    error = NetworkError("private remote description") if explicit else RetryAfter(7)
    with caplog.at_level(logging.WARNING, logger="app.telegram_bots.supervisor"):
        log_call_failure(link_id="link", bot_id=1234567, method="answerCallbackQuery", error=error, retry_after_seconds=80 if explicit else None)
    message = caplog.records[0].getMessage()
    assert message.startswith("telegram_bots retry_after link=link bot=1234567 ")
    assert "retry_in=" + ("80" if explicit else "7") in message
    assert message.index("method=") < message.index("retry_in=") < message.index("attempts=")
    assert all(key not in message for key in ("error=", "cause=", "http=", "desc="))
    assert "private" not in message and "outbox_id=" not in message
