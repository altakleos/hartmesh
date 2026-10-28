"""Fixed ordinary-chat outcomes from correlated host evidence, never wording."""

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from app.channels.execution import ChannelExecutionProtocolError, ChannelExecutionScope
from app.channels.execution_client import ValidatedRun
from app.channels.outcomes import CreationRefusal, validate_creation_refusal
from app.gateway.channel_execution import ChannelExecutionReceipt, receipt_from_run_kwargs

RUN_STOP_REASONS: Mapping[str, str] = MappingProxyType({"sandbox_capacity_exceeded": "busy.queued", "files-full": "chat.files_full", "orphan_recovered": "refused.interrupted"})
STORAGE_REFERENCE_WORDS: Mapping[str, str] = MappingProxyType(
    {
        "disk-too-small": "disk-too-small",
        "storage-pool-missing": "storage-missing",
        "inventory-unreadable": "storage-unreadable",
        "host-unqualified": "server-not-ready",
        "admission-failed": "server-check-failed",
        "host-replaced": "server-moved",
        "space-unverified": "files-unchecked",
    }
)

# Legacy display vocabulary and source pins only. A matching sentence grants
# neither creating-refusal provenance nor permission to retry a publication.
MANAGER_SENTENCES: Mapping[str, str] = MappingProxyType(
    {
        "This conversation is already processing another request. Please wait for it to finish and try again.": "busy.thread",
        "Connect this channel from Settings in the web app, complete the in-channel connect step, then send your message again.": "access.off",
        "Channel connection verification is temporarily unavailable. Please try again later or contact your administrator.": "error.generic",
        "An internal error occurred. Please try again.": "error.generic",
        "This conversation has an invalid stored agent selection. Use /agent use <name> to start a valid conversation.": "error.generic",
        "An error occurred while processing your request. Please try again.": "error.generic",
        "(No response from agent)": "error.generic",
        "Channel execution protocol refused": "error.generic",
    }
)


@dataclass(frozen=True)
class TurnFailure:
    text_key: str
    retry_later: bool = False


def _refuse():
    raise ChannelExecutionProtocolError() from None


def classify_run(run: ValidatedRun, scope: ChannelExecutionScope) -> TurnFailure | None:
    """Classify a terminal snapshot already read for this exact publication.

    This selects product wording; the controller still owns current authority,
    the retry deadline, fresh publication allocation and once-only completion.
    """
    try:
        if type(run) is not ValidatedRun or type(scope) is not ChannelExecutionScope or scope.assistant_id != "lead_agent" or type(run.receipt) is not ChannelExecutionReceipt:
            _refuse()
        row, proof = run.row, run.receipt
        if (
            not isinstance(row, Mapping)
            or type(row.get("run_id")) is not str
            or not row["run_id"]
            or row.get("thread_id") != scope.thread_id
            or any(key in row and row[key] != scope.owner_user_id for key in ("user_id", "owner_user_id"))
            or (proof.owner_user_id, proof.thread_id, proof.publication_id, proof.assistant_id, proof.agent_name) != (scope.owner_user_id, scope.thread_id, scope.publication_id, scope.assistant_id, scope.agent_name)
            or receipt_from_run_kwargs(row.get("kwargs")) != proof
            or not isinstance(row.get("metadata"), Mapping)
            or row["metadata"].get("channel_turn_id") != scope.publication_id
        ):
            _refuse()
        alias = row.get("assistant_id")
        if alias not in (None, scope.assistant_id) and (scope.agent_name is None or type(alias) is not str or alias.strip().lower().replace("_", "-") != scope.agent_name):
            _refuse()
        status, reason = row.get("status"), row.get("stop_reason")
        if type(status) is not str or status not in {"success", "error", "timeout", "interrupted"} or reason is not None and type(reason) is not str:
            _refuse()
        if status == "success":
            return None
        if reason == "orphan_recovered":
            return TurnFailure(RUN_STOP_REASONS[reason])
        if status == "error" and reason in RUN_STOP_REASONS:
            return TurnFailure(RUN_STOP_REASONS[reason], reason == "sandbox_capacity_exceeded")
        # An externally interrupted run is not evidence of this chat's Stop.
        # Explicit original-turn Stop has its separate owned completion path.
        return TurnFailure("error.generic")
    except Exception:
        _refuse()


def classify_creation_refusal(proof: CreationRefusal, scope: ChannelExecutionScope) -> TurnFailure:
    """Only the captured original host refusal may select thread waiting."""
    if type(scope) is not ChannelExecutionScope or scope.assistant_id != "lead_agent":
        _refuse()
    validate_creation_refusal(proof, scope)
    return TurnFailure("busy.thread", True)
