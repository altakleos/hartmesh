"""Closed API diagnostics shared by transport, polling and delivery recovery."""

import logging

from telegram.error import BadRequest, Conflict, Forbidden, InvalidToken, RetryAfter

logger = logging.getLogger("app.telegram_bots.supervisor")


def call_failure_fields(error: BaseException) -> dict:
    fields = {"error": type(error).__name__}
    if error.__cause__ is not None:
        fields["cause"] = type(error.__cause__).__name__
    if isinstance(error, RetryAfter):
        fields["http"] = 429
    elif isinstance(error, InvalidToken):
        actual = error.__cause__ if isinstance(error.__cause__, InvalidToken) else error
        description = str(actual).strip().lower()
        if description in {"unauthorized", "not found"}:
            fields["http"] = 401 if description == "unauthorized" else 404
    elif isinstance(error, (Conflict, Forbidden, BadRequest)):
        fields["http"] = 409 if isinstance(error, Conflict) else 403 if isinstance(error, Forbidden) else 400
        description = str(error).lower()  # Classification only; never emitted.
        cases = (
            ("other-poller", "terminated by other getupdates" in description),
            ("webhook", isinstance(error, Conflict) and "webhook" in description),
            ("blocked", "blocked by the user" in description),
            ("not-started", "can't initiate conversation" in description),
            ("parse-entities", "can't parse entities" in description),
            ("not-modified", "message is not modified" in description),
            ("edit-anchor-missing", any(text in description for text in ("message to edit not found", "message can't be edited", "message cannot be edited"))),
            ("query-too-old", "query is too old" in description),
            ("thread-not-found", "message thread not found" in description),
            ("button-url", "url" in description and "invalid" in description),
            ("too-big", "file is too big" in description),
        )
        fields["desc"] = next((name for name, matches in cases if matches), "other")
    return fields


def log_call_failure(
    *,
    link_id: str,
    bot_id: int | None,
    method: str,
    error: BaseException,
    attempts: int = 1,
    outbox_id: str | None = None,
    part_id: str | None = None,
    class_: str | None = None,
    claim_owner: str | None = None,
    claim_id: str | None = None,
    failed_reason: str | None = None,
    retry_after_seconds: float | None = None,
) -> None:
    if isinstance(error, RetryAfter):
        retry_after_seconds = error._retry_after.total_seconds()
    fields = {"link": link_id, "bot": bot_id, "method": method}
    if retry_after_seconds is not None:
        fields.update(retry_in=retry_after_seconds, attempts=attempts)
    else:
        diagnostic = call_failure_fields(error)
        description = diagnostic.pop("desc", None)
        fields.update(diagnostic, attempts=attempts, desc=description)
    fields.update(outbox_id=outbox_id, part_id=part_id, **{"class": class_}, claim_owner=claim_owner, claim_id=claim_id)
    if retry_after_seconds is None:
        fields["failed_reason"] = failed_reason
    event = "retry_after" if retry_after_seconds is not None else "send_failed"
    logger.warning("telegram_bots %s %s", event, " ".join(f"{key}={value}" for key, value in fields.items() if value is not None))
    error._hartmesh_telegram_failure_logged = True
