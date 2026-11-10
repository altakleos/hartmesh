"""Strict channel lookup scope, separate from ordinary compatibility reads."""

from collections.abc import Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from types import SimpleNamespace

from fastapi import HTTPException, Request

from app.gateway.auth.mode import account_refusal
from app.gateway.auth_disabled import AUTH_SOURCE_INTERNAL
from app.gateway.channel_execution import CHANNEL_EXECUTION_REQUEST_HEADER, CHANNEL_EXECUTION_RESPONSE_HEADER, CHANNEL_EXECUTION_VERSION, ChannelExecutionProofError, receipt_from_run_kwargs
from app.gateway.deps import get_local_provider, get_thread_store
from app.gateway.internal_auth import INTERNAL_SYSTEM_ROLE, get_trusted_internal_owner_user_id
from deerflow.runtime.user_context import reset_current_user, set_current_user
from deerflow.utils.file_io import await_drained, run_file_io


def strict_channel_read(request: Request) -> bool:
    headers = request.headers
    values = headers.getlist(CHANNEL_EXECUTION_REQUEST_HEADER) if hasattr(headers, "getlist") else [headers.get(CHANNEL_EXECUTION_REQUEST_HEADER)]
    values = [value for value in values if value is not None]
    if not values:
        return False
    if values != [str(CHANNEL_EXECUTION_VERSION)]:
        raise HTTPException(422, "Unsupported channel execution read version")
    return True


def channel_read_headers() -> dict[str, str]:
    return {CHANNEL_EXECUTION_RESPONSE_HEADER: str(CHANNEL_EXECUTION_VERSION)}


def channel_lookup_unavailable() -> HTTPException:
    return HTTPException(503, "Channel execution lookup is unavailable")


@dataclass(frozen=True, eq=False)
class OrdinaryMemoryThreads:
    """Startup proof of an ordinary memory store's no-instance lifetime."""

    store: object
    _retired: bool = field(default=False, init=False)

    def __post_init__(self):
        from deerflow.persistence.thread_meta.memory import MemoryThreadMetaStore

        if type(self.store) is not MemoryThreadMetaStore:
            raise TypeError("Ordinary memory thread authority requires its exact store")

    def retire(self):
        object.__setattr__(self, "_retired", True)


async def _thread_scope(request, thread_id, owner):
    from deerflow.agent_instances.contract import AgentPermission
    from deerflow.agent_instances.conversations import AgentConversations
    from deerflow.spaces.contract import PrincipalRef

    store = get_thread_store(request)
    authority = getattr(request.app.state, "agent_conversations", None)
    memory = getattr(request.app.state, "channel_memory_threads", None)
    try:
        if type(authority) is AgentConversations:
            if type(memory) is OrdinaryMemoryThreads:
                memory.retire()
        elif not (authority is None and type(memory) is OrdinaryMemoryThreads and not memory._retired and memory.store is store):
            raise channel_lookup_unavailable()
        row = await store.get(thread_id, user_id=None)
        if not isinstance(row, Mapping) or row.get("thread_id") != thread_id or row.get("user_id") != owner:
            raise ChannelReadDenied()
        binding = await authority.binding(thread_id) if authority is not None else None
        if binding is not None and not await authority.allowed(actor=PrincipalRef("human", owner), thread_id=thread_id, permission=AgentPermission.INSPECT):
            raise ChannelReadDenied()
        if (
            get_thread_store(request) is not store
            or getattr(request.app.state, "agent_conversations", None) is not authority
            or getattr(request.app.state, "channel_memory_threads", None) is not memory
            or authority is None
            and memory._retired
        ):
            raise ChannelReadDenied()
    except ChannelReadDenied:
        raise
    except Exception:
        raise channel_lookup_unavailable() from None
    return store, authority, memory, binding, row.get("incarnation")


class ChannelReadDenied(HTTPException):
    """Only this explicit scope denial bypasses the backend error boundary."""

    def __init__(self):
        super().__init__(404, "Run not found")


class ChannelReadMissing(HTTPException):
    """A completed supported lookup found no run in the admitted scope."""

    def __init__(self):
        super().__init__(404, "Run not found", headers=channel_read_headers())


async def live_channel_human(request: Request) -> str:
    """Derive the real human from authenticated state, never a default bucket."""
    user = getattr(request.state, "user", None)
    if getattr(user, "system_role", None) == INTERNAL_SYSTEM_ROLE:
        if getattr(request.state, "auth_source", None) != AUTH_SOURCE_INTERNAL:
            raise HTTPException(403, "Channel execution requires an authenticated human owner")
        owner = get_trusted_internal_owner_user_id(request)
        if not owner:
            raise HTTPException(403, "Channel execution requires an authenticated human owner")
        try:
            user = await get_local_provider().get_user(owner)
        except Exception:
            raise channel_lookup_unavailable() from None
    else:
        owner = str(user.id) if getattr(user, "id", None) is not None else None
    if not owner or user is None or str(getattr(user, "id", "")) != owner or getattr(user, "system_role", None) not in {"user", "admin"}:
        raise HTTPException(403, "Channel execution requires an authenticated human owner")
    try:
        # Config resolution may touch a cold file. This does not impose the
        # session-only setup restriction on existing internal automations.
        refused = await await_drained(run_file_io(account_refusal, user))
    except Exception:
        raise channel_lookup_unavailable() from None
    if refused is not None:
        raise HTTPException(403, "Channel execution owner is unavailable")
    return owner


@asynccontextmanager
async def channel_read_scope(request: Request, thread_id: str):
    owner = await live_channel_human(request)
    token = set_current_user(SimpleNamespace(id=owner))
    try:
        captured = await await_drained(_thread_scope(request, thread_id, owner))

        async def refresh():
            if await live_channel_human(request) != owner or await await_drained(_thread_scope(request, thread_id, owner)) != captured:
                raise ChannelReadDenied()

        # In particular, DbRunEventStore's AUTO identity must see this raw
        # human owner rather than an internal request's path-safe alias.
        try:
            yield owner
        except ChannelReadMissing:
            # Supported absence acknowledges authority just like returned data.
            await refresh()
            raise
        else:
            # Recheck after awaited source/store work before acknowledging data.
            await refresh()
    finally:
        reset_current_user(token)


def validate_channel_record(record, *, owner: str, thread_id: str) -> None:
    if record.thread_id != thread_id or record.user_id != owner:
        raise ChannelReadDenied()
    receipt = receipt_from_run_kwargs(record.kwargs)
    if receipt is None:
        return
    if receipt.owner_user_id != owner or receipt.thread_id != thread_id or receipt.assistant_id != "lead_agent":
        raise ChannelExecutionProofError()
    alias = record.assistant_id
    if alias not in (None, "lead_agent") and (type(alias) is not str or alias.strip().lower().replace("_", "-") != receipt.agent_name):
        raise ChannelExecutionProofError()


def validate_channel_messages(rows, *, owner: str, thread_id: str, run_id: str, limit: int, before_seq: int | None, after_seq: int | None) -> None:
    if not isinstance(rows, list) or len(rows) > limit:
        raise ChannelExecutionProofError()
    previous = 0
    for row in rows:
        if not isinstance(row, Mapping) or row.get("thread_id") != thread_id or row.get("run_id") != run_id or ("user_id" in row and row["user_id"] != owner):
            raise ChannelExecutionProofError()
        seq = row.get("seq")
        if type(seq) is not int or seq <= previous or (before_seq is not None and seq >= before_seq) or (after_seq is not None and seq <= after_seq):
            raise ChannelExecutionProofError()
        previous = seq
        if (
            row.get("category") != "message"
            or type(row.get("event_type")) is not str
            or not row["event_type"]
            or not isinstance(row.get("content"), (str, dict))
            or not isinstance(row.get("metadata"), Mapping)
            or type(row.get("created_at")) is not str
        ):
            raise ChannelExecutionProofError()
        timestamp = datetime.fromisoformat(row["created_at"])
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ChannelExecutionProofError()
