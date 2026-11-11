"""Fixed ordinary-chat outcomes from correlated host evidence, never wording."""

from collections.abc import Mapping
from dataclasses import dataclass, replace
from types import MappingProxyType

from app.channels.execution import ChannelExecutionProtocolError, ChannelExecutionScope
from app.channels.execution_client import ValidatedRun
from app.channels.outcomes import CreationRefusal, validate_creation_refusal
from app.gateway.channel_execution import ChannelExecutionReceipt, receipt_from_run_kwargs
from deerflow.runtime.runs.terminal import MODEL_REFUSED_STOP_REASON

RUN_STOP_REASONS: Mapping[str, str] = MappingProxyType(
    {"sandbox_capacity_exceeded": "busy.queued", "files-full": "chat.files_full", "orphan_recovered": "refused.interrupted", "gateway_stopped": "chat.restarted", MODEL_REFUSED_STOP_REASON: "error.model_refused"}
)
INSTANCE_RUN_STOP_REASONS: Mapping[str, str] = MappingProxyType(
    {
        "busy": "busy.queued",
        "starting-elsewhere": "busy.starting_elsewhere",
        "run-limit": "busy.two_conversations",
        "restarting": "refused.restarting",
        "storage-busy": "refused.files_busy",
        "start-failed": "start.ws_failed",
        "files-full": "refused.files_full",
    }
)
CAPACITY_KEYS = frozenset({"busy.queued", "busy.thread", "busy.starting_elsewhere", "busy.two_conversations"})
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


@dataclass(frozen=True)
class TurnFailure:
    text_key: str
    retry_later: bool = False


def _refuse():
    raise ChannelExecutionProtocolError() from None


async def read_error_evidence(run, scope, facade):
    """Enrich only an unclassified instance error, using its original facade."""
    if run is not None and scope.instance_id is not None and run.row.get("status") == "error" and classify_run(run, scope).text_key == "error.generic":
        return replace(run, error_evidence=await facade.get_run_error(run.row["run_id"]))
    return run


def classify_run(run: ValidatedRun, scope: ChannelExecutionScope) -> TurnFailure | None:
    from app.channels.run_errors import validate_run_error

    try:
        facts = validate_run_error(run.error_evidence, scope, run.row["run_id"]) if type(run) is ValidatedRun and run.error_evidence is not None else None
        return _classify_run(run, scope, facts)
    except Exception:
        _refuse()


def classify_saved_run(run, scope, saved_error):
    """Saved server facts select wording without issuing a live capability."""
    from app.channels.run_errors import validate_saved_run_error

    return _classify_run(run, scope, validate_saved_run_error(saved_error, scope, run.row["run_id"]))


def _classify_run(run: ValidatedRun, scope: ChannelExecutionScope, facts=None) -> TurnFailure | None:
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
        if reason in {"orphan_recovered", "gateway_stopped"}:
            return TurnFailure(RUN_STOP_REASONS[reason])
        if status == "error" and scope.instance_id is not None and reason in INSTANCE_RUN_STOP_REASONS:
            key = INSTANCE_RUN_STOP_REASONS[reason]
            return TurnFailure(key, key in CAPACITY_KEYS)
        if status == "error" and reason in RUN_STOP_REASONS:
            return TurnFailure(RUN_STOP_REASONS[reason], reason == "sandbox_capacity_exceeded")
        if status == "error" and scope.instance_id is not None and facts is not None:
            if facts["name"] == "ResidentInterrupted":
                return TurnFailure({"lifecycle": "refused.interrupted_manager", "storage-recovery": "refused.interrupted_recovery"}.get(facts["reason"], "refused.interrupted"))
            if facts["name"] == "Refusal" and facts["reason"] == "managers-only":
                return TurnFailure("net.blocked_ask")
        # An externally interrupted run is not evidence of this chat's Stop.
        # Explicit original-turn Stop has its separate owned completion path.
        return TurnFailure("error.generic")
    except Exception:
        _refuse()


def classify_creation_refusal(proof: CreationRefusal, scope: ChannelExecutionScope) -> TurnFailure:
    """Only the captured original host refusal may select thread waiting."""
    if type(scope) is not ChannelExecutionScope or scope.assistant_id != "lead_agent":
        _refuse()
    from app.telegram_bots.instance_refusals import RecheckedCreationRefusal, validate_rechecked_refusal

    if type(proof) is RecheckedCreationRefusal:
        failure = validate_rechecked_refusal(proof, scope)
        return TurnFailure(failure.text_key, failure.retry_later)
    validate_creation_refusal(proof, scope)
    return TurnFailure("busy.thread", True)


WORK_DENIALS: Mapping[str, str] = MappingProxyType({"respond": "denied.respond", "decide": "denied.decide", "accept": "denied.accept", "changes_requested": "denied.changes", "route": "denied.route"})
READER_FORMS: Mapping[str, tuple[str, str]] = MappingProxyType(
    {
        "refused.files_full": ("refused.files_full", "refused.files_full_ask"),
        "file.files_full": ("file.files_full", "file.files_full_ask"),
        "ask.cant_saved": ("ask.cant_saved_manager", "ask.cant_saved"),
    }
)


def key_for_reader(key, permissions):
    forms = READER_FORMS.get(key)
    return forms[0 if permissions & 4 else 1] if forms else key


def text_key_for(kind, exc):
    """Classify a saved Work call's refusal; unknown outcomes are never failures.

    Routing conflicts need a separate current-request read to choose refresh.
    This function neither authorizes a retry nor inspects exception sentences.
    """
    from app.telegram_bots.authority import AccessOff, CeilingRefused
    from deerflow.agent_instances.contract import AgentConflict, AgentDenied, Refusal

    if isinstance(exc, (AgentDenied, AgentConflict, Refusal)):
        reason = exc.reason
        if reason == "actions-paused" and kind in {"decide", "accept"}:
            return "link.limited"
        if reason == "attempt-unresolved":
            return "decide.wait" if kind in {"decide", "accept"} else "tap.failed"
    if isinstance(exc, (AgentDenied, CeilingRefused, AccessOff)):
        return WORK_DENIALS.get(kind, "denied.other")
    if isinstance(exc, AgentConflict):
        return "stale.changed_resend" if kind in {"respond", "decide", "accept", "changes_requested"} else "tap.failed"
    if isinstance(exc, Refusal):
        return "tap.failed"
    return "act.unconfirmed"
