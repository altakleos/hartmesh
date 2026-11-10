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


_CREATION_ISSUER = object()
_CREATION_KEYS = frozenset({"busy.thread", "busy.two_conversations", "refused.restarting", "refused.files_busy", "unavailable.off", "unavailable.paused", "unavailable.moved", "unavailable.files_unchecked", "unavailable.fix"})
_REFERENCE_KEYS = frozenset({"unavailable.moved", "unavailable.files_unchecked", "unavailable.fix"})


@dataclass(frozen=True, init=False)
class RecheckedCreationRefusal:
    """Original acknowledged refusal paired with actual instance admission."""

    _issuer: object
    _original: object
    _scope: object
    _key: str
    _code: str | None


def _saved_failure(key, code):
    from app.channels.execution import ChannelExecutionProtocolError

    if type(key) is not str or key not in _CREATION_KEYS or code is not None and (type(code) is not str or code not in STORAGE_REFERENCE_WORDS.values() or key not in _REFERENCE_KEYS):
        raise ChannelExecutionProtocolError()
    return SendFailure(key, key in {"busy.thread", "busy.two_conversations"}, {"code": code} if code else {})


def validate_rechecked_refusal(proof, scope) -> SendFailure:
    from app.channels.execution import ChannelExecutionProtocolError
    from app.channels.outcomes import validate_creation_refusal

    if type(proof) is not RecheckedCreationRefusal or getattr(proof, "_issuer", None) is not _CREATION_ISSUER or proof._scope != scope or scope.instance_id is None:
        raise ChannelExecutionProtocolError()
    validate_creation_refusal(proof._original, scope)
    return _saved_failure(proof._key, proof._code)


def saved_rechecked_refusal(proof, scope):
    from dataclasses import asdict

    failure = validate_rechecked_refusal(proof, scope)
    return {"kind": "instance_creation_refusal", "scope": asdict(scope), "key": failure.text_key, "code": failure.values.get("code")}


def read_saved_recheck(proof, scope):
    from dataclasses import asdict

    from app.channels.execution import ChannelExecutionProtocolError

    if type(proof) is not dict or set(proof) != {"kind", "scope", "key", "code"} or proof["kind"] != "instance_creation_refusal" or scope.instance_id is None or proof["scope"] != asdict(scope):
        raise ChannelExecutionProtocolError()
    return _saved_failure(proof["key"], proof["code"])


async def recheck_creation_refusal(host, link, record, original):
    """Recheck the original bound conversation; this creates/reserves nothing."""
    from app.channels.execution import ChannelExecutionProtocolError
    from app.channels.outcomes import validate_creation_refusal
    from app.telegram_bots.publication import channel_execution_scope
    from deerflow.spaces.contract import PrincipalRef

    scope = channel_execution_scope(record)
    validate_creation_refusal(original, scope)
    fields = ("link_id", "owner_user_id", "agent_kind", "agent_ref", "connection_id", "chat_id", "binding_generation")
    expected = (link.id, link.owner_user_id, link.agent_kind, link.agent_ref, link.connection_id, link.chat_id, link.binding_generation)
    if scope.instance_id is None or tuple(getattr(record.scope, field) for field in fields) != expected:
        raise ChannelExecutionProtocolError()

    async def perform():
        attempt = await host.publication_authority.capture(link, thread_id=scope.thread_id, endpoint_id=scope.endpoint_id)
        await attempt.admission.authorize("run_read")
        conversations = attempt.admission.conversations
        try:
            execution = await conversations.execution(actor=PrincipalRef("human", link.owner_user_id), thread_id=scope.thread_id, require_ready=True)
            if execution.instance.id != scope.instance_id:
                raise ChannelExecutionProtocolError()
            residents = conversations.instances.residents
            if residents is not None:
                await residents.admit(execution.instance, work=False)
        except Refusal as error:
            if error.status != 409:
                raise
            failure = await describe_send_refusal(host.authority, link, error)
        else:
            failure = SendFailure("busy.thread", True)
        await attempt.admission.authorize("run_read")
        result = object.__new__(RecheckedCreationRefusal)
        for name, value in {"_issuer": _CREATION_ISSUER, "_original": original, "_scope": scope, "_key": failure.text_key, "_code": failure.values.get("code")}.items():
            object.__setattr__(result, name, value)
        validate_rechecked_refusal(result, scope)
        return result

    return await await_drained(perform())
