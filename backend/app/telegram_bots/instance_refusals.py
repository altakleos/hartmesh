"""Fixed send failures from actual instance admission, never manager wording."""

from dataclasses import dataclass, field

from app.telegram_bots.errors import STORAGE_REFERENCE_WORDS
from deerflow.agent_instances.contract import Refusal
from deerflow.utils.file_io import await_drained

SEND_REFUSALS = frozenset({"execution-unavailable", "restarting", "run-limit", "storage-busy"})


@dataclass(frozen=True)
class SendFailure:
    text_key: str
    retry_later: bool = False
    values: dict = field(default_factory=dict)


async def describe_send_refusal(authority, link, error: Refusal) -> SendFailure:
    if not isinstance(error, Refusal) or error.reason not in SEND_REFUSALS:
        raise error
    # Recheck Use before reading storage diagnostics, including a grant change
    # during the original readiness call. Counts never govern an existing bot.
    await await_drained(authority.instance_view(link))
    if error.reason != "execution-unavailable":
        return SendFailure({"restarting": "refused.restarting", "run-limit": "busy.two_conversations", "storage-busy": "refused.files_busy"}[error.reason], error.reason == "run-limit")
    reason = error.extra.get("execution_reason")
    key = {"agents-none": "unavailable.off", "operator-hold": "unavailable.paused", "host-replaced": "unavailable.moved", "space-unverified": "unavailable.files_unchecked"}.get(reason, "unavailable.fix")
    if reason == "storage-unavailable":
        from app.gateway.storage_spaces import storage_state

        reason = (await await_drained(storage_state(authority.app))).reason
    code = STORAGE_REFERENCE_WORDS.get(reason)
    return SendFailure(key, values={"code": code} if code and key in {"unavailable.moved", "unavailable.files_unchecked", "unavailable.fix"} else {})
