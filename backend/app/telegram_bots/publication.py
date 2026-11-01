"""Callable managed turn handoff and exact bounded observation.

This installs no input handler or scheduler. Model frames are never a final
source, and an irreversible dispatch marker never permits an automatic retry.
"""

from __future__ import annotations

import asyncio
import math
import re
import weakref
from collections.abc import Awaitable, Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal
from urllib.parse import quote

from app.channels.execution import ChannelExecutionContext, ChannelExecutionScope, ChannelHttpOwner
from app.channels.execution_client import ChannelExecutionFacade, ValidatedRun
from app.channels.message_bus import InboundMessage, InboundMessageType, MessageBus
from app.gateway.run_output import RunOutputProjectionV2
from app.telegram_bots.inbox import InboxRepository, InboxRow, PublicationRecord, PublicationScope, chat_input_parts, validate_chat_file
from app.telegram_bots.links import BotLink, prepare_publication_guard
from app.telegram_bots.producer import Outbox, freeze_reply_presentation
from deerflow.runtime.runs.output_inventory import delivery_items
from deerflow.utils.file_io import await_drained

if TYPE_CHECKING:
    from app.telegram_bots.publication_authority import PublicationAttempt

OBSERVATION_TIMEOUT_SECONDS = 30
OUTPUT_READ_TIMEOUT_SECONDS = 30
OBSERVATION_PAGE_SIZE = 100
MAX_OBSERVATION_PAGES = 8
_OPERATIONS = frozenset({"thread_read", "run_create", "run_read", "source_read"})


class PublicationRefused(RuntimeError):
    def __init__(self):
        super().__init__("Managed publication refused")


def _refuse():
    raise PublicationRefused() from None


@dataclass(frozen=True)
class PublicationObservation:
    state: Literal["pending", "planned", "held", "budget_exhausted"]
    record: PublicationRecord
    outbox_id: str | None = None
    reason: Literal["awaiting_run", "awaiting_output", "source_unavailable", "inventory_unhandled", "unsupported_presentation", "ambiguous_run", "budget_exhausted"] | None = None

    def __post_init__(self):
        allowed = {
            "pending": {"awaiting_run", "awaiting_output"},
            "planned": {None},
            "held": {"source_unavailable", "inventory_unhandled", "unsupported_presentation", "ambiguous_run"},
            "budget_exhausted": {"budget_exhausted"},
        }
        if type(self.record) is not PublicationRecord or type(self.state) is not str or self.state not in allowed or self.reason not in allowed[self.state]:
            _refuse()
        if self.state == "planned":
            if type(self.outbox_id) is not str or re.fullmatch(r"[0-9a-f]{32}", self.outbox_id) is None:
                _refuse()
        elif self.outbox_id is not None:
            _refuse()


@dataclass(frozen=True)
class PublicationRunObservation:
    state: Literal["pending", "matched", "held", "budget_exhausted"]
    record: PublicationRecord
    run: ValidatedRun | None = None
    reason: str | None = None

    def __post_init__(self):
        allowed = {"pending": {"awaiting_run"}, "matched": {None}, "held": {"ambiguous_run", "source_unavailable"}, "budget_exhausted": {"budget_exhausted"}}
        if type(self.record) is not PublicationRecord or self.state not in allowed or self.reason not in allowed[self.state] or (type(self.run) is not ValidatedRun if self.state == "matched" else self.run is not None):
            _refuse()
        if self.state == "matched" and self.record.run_id != self.run.row.get("run_id"):
            _refuse()


@dataclass
class _Control:
    retired: bool = False
    final_check: Callable[[], None] | None = None
    deadline: float | None = None
    clock: Callable[[], float] | None = None

    def in_budget(self):
        return self.deadline is None or self.clock() < self.deadline

    def check_budget(self):
        if not self.in_budget():
            raise _DeadlineExceeded() from None


class _DeadlineExceeded(Exception):
    pass


@dataclass(frozen=True)
class _Admission:
    coordinator: PublicationCoordinator
    control: _Control
    actual: object
    thread_id: str
    endpoint_id: str

    def current(self, operation):
        try:
            return bool(
                type(operation) is str
                and operation in _OPERATIONS
                and not self.control.retired
                and self.control.in_budget()
                and self.coordinator._reading
                and (operation != "run_create" or self.coordinator._creating)
                and self.actual.current(operation) is True
            )
        except Exception:
            return False

    def check(self, operation):
        self.control.check_budget()
        if self.current(operation) is not True:
            _refuse()

    async def authorize(self, operation):
        self.check(operation)
        await await_drained(self.actual.authorize(operation))
        self.check(operation)


@dataclass(frozen=True)
class _Guard:
    actual: object
    admission: _Admission
    operation: str

    @property
    def captured_link(self):
        return self.actual.captured_link

    def current(self):
        """Synchronous original-operation fence for composed host readers."""
        return self.admission.current(self.operation)

    async def prepare(self) -> None:
        self.admission.check(self.operation)
        await prepare_publication_guard(self.actual)
        self.admission.check(self.operation)

    async def check(self, writer):
        self.admission.check(self.operation)
        result = await self.actual.check(writer)
        # No authority await follows this local original-operation fence.
        self.admission.check(self.operation)
        return result


def _scope(link, *, thread_id, endpoint_id, topic):
    return PublicationScope(
        link_id=link.id,
        owner_user_id=link.owner_user_id,
        bot_id=link.bot_id,
        agent_kind=link.agent_kind,
        agent_ref=link.agent_ref,
        assistant_id="lead_agent",
        agent_name=link.agent_ref if link.agent_kind == "custom_agent" else None,
        connection_id=link.connection_id,
        telegram_user_id=link.telegram_user_id,
        chat_id=link.chat_id,
        message_thread_id=topic,
        binding_generation=link.binding_generation,
        thread_id=thread_id,
        endpoint_id=endpoint_id,
    )


def _plain_origins(link, origins, *, thread_id, endpoint_id):
    if type(link) is not BotLink or not isinstance(origins, Sequence) or isinstance(origins, (str, bytes)) or not origins or any(type(row) is not InboxRow for row in origins):
        _refuse()
    scope = _scope(link, thread_id=thread_id, endpoint_id=endpoint_id, topic=origins[0].message_thread_id)
    identities = set()
    for row in origins:
        if (row.link_id, row.chat_id, row.message_thread_id, row.binding_generation) != (scope.link_id, scope.chat_id, scope.message_thread_id, scope.binding_generation) or (row.route_kind, row.route_target_id) != ("turn", scope.agent_ref):
            _refuse()
        identity = row.link_id, row.message_id, row.chat_id
        if identity in identities or type(row.accepted_at) is not datetime or row.accepted_at.tzinfo is None:
            _refuse()
        identities.add(identity)
        if type(row.route_basis) is not dict:
            _refuse()
        try:
            chat_input_parts(row, bot_id=link.bot_id)
        except (KeyError, TypeError, ValueError):
            _refuse()
    return tuple(sorted(origins, key=lambda row: (row.accepted_at.astimezone(UTC), row.update_id, row.message_id)))


def _try_again(record):
    return [[{"label": "label.try_again", "kind": "retry_message", "target_kind": "attempt", "target_id": record.publication_id}]]


async def fixed_source(host, link, key):
    """A fixed sentence with the values it needs. `host` is the supervisor or anything shaped like it."""
    values = {}
    if key == "custom_agent.deleted":
        values = {"product": host.product_name, "agent": link.bot_username or host.product_name}
    elif key == "chat.files_full":
        values = {"product": host.product_name, "support": "an administrator"}
    elif key == "refused.interrupted":
        values = {"name": (await host.prepare_ingress_values(link))["name"]}
    return {"key": key, "values": values}


async def conversation_button(link, record):
    """The way to this conversation on the web, when the web has an address."""
    from app.telegram_bots.pages import page_address

    address = await await_drained(asyncio.to_thread(page_address, "web.conversation", link, thread_id=record.scope.thread_id))
    return {"page": "web.conversation", "url": address} if address else None


def channel_execution_scope(record: PublicationRecord) -> ChannelExecutionScope:
    if type(record) is not PublicationRecord:
        _refuse()
    return _channel_scope(record)


def _channel_scope(record):
    scope = record.scope
    return ChannelExecutionScope(record.publication_id, scope.owner_user_id, scope.connection_id, str(scope.chat_id), scope.thread_id, scope.endpoint_id, scope.assistant_id, scope.agent_name)


class PublicationCoordinator:
    def __init__(self, *, inbox: InboxRepository, outbox: Outbox, bus: MessageBus, http_owner: ChannelHttpOwner, capture_attempt: Callable[..., Awaitable[PublicationAttempt]]):
        self.inbox, self.outbox, self.bus = inbox, outbox, bus
        self._http_owner, self._capture_attempt = http_owner, capture_attempt
        self._creating = self._reading = True
        self._operations: set[asyncio.Task] = set()
        self._stop_task = None
        # Held by the queued message, so an entry lasts exactly as long as that message.
        self._queued: weakref.WeakValueDictionary[str, ChannelExecutionContext] = weakref.WeakValueDictionary()
        self.human_input_planner = None
        self.file_output_planner = None
        self.file_preflight = None
        self.host = None

    def fence(self, record: PublicationRecord) -> None:
        """Refuse a queued message's POST, so a worker that reaches it later cannot run it."""
        context = self._queued.get(record.publication_id)
        if context is not None:
            context.admission.control.retired = True

    def worker_state(self, record: PublicationRecord) -> Literal["queued", "taken"] | None:
        """Where a marked message is: waiting for a worker, with one, or held by nobody.

        A fence turns back a message `queued`. One `taken` can have a POST on the wire that no
        fence recalls, so only its worker letting go proves no run is coming.
        """
        context = self._queued.get(record.publication_id)
        if context is None or context.progress.released:
            return None
        return "taken" if context.progress.taken else "queued"

    def _check_open(self, *, creating):
        if not self._reading or creating and not self._creating:
            _refuse()

    async def _call(self, factory, *, creating=False, timeout=None, expired=None, deadline=None):
        self._check_open(creating=creating)
        loop = asyncio.get_running_loop()
        end = None if timeout is None else loop.time() + timeout
        if deadline is not None:
            end = deadline if end is None else min(end, deadline)
        control = _Control(deadline=end, clock=loop.time)
        if not control.in_budget():
            if expired is not None:
                return expired()
            _refuse()

        async def run():
            # The child may be queued while the parent's timer is delayed too.
            control.check_budget()
            return await factory(control)

        task = asyncio.create_task(run(), name="telegram-publication-operation")
        self._operations.add(task)
        task.add_done_callback(self._operations.discard)
        try:
            if timeout is None:
                result = await asyncio.shield(task)
                return self._handoff(result, control)
            done, _ = await asyncio.wait({task}, timeout=max(0, control.deadline - loop.time()))
            if not done:
                control.retired = True
                task.cancel()
                await self._settle(task)
                if expired is not None:
                    return expired()
                _refuse()
            return self._handoff(task.result(), control)
        except asyncio.CancelledError:
            control.retired = True
            task.cancel()
            await self._settle(task)
            raise
        except _DeadlineExceeded:
            control.retired = True
            if not task.done():
                task.cancel()
                await self._settle(task)
            if expired is not None:
                return expired()
            _refuse()
        except Exception:
            _refuse()

    @staticmethod
    def _handoff(result, control):
        # The child may settle before its public waiter resumes. Buffered
        # private output still needs its original synchronous admission here.
        control.check_budget()
        if control.final_check is not None:
            control.final_check()
        return result

    @staticmethod
    async def _settle(task):
        async def join():
            await asyncio.gather(task, return_exceptions=True)

        await await_drained(join())

    async def _attempt(self, link, record_scope, control):
        control.check_budget()
        if self._http_owner.endpoint_id != record_scope.endpoint_id:
            _refuse()
        actual = await self._capture_attempt(link, thread_id=record_scope.thread_id, endpoint_id=record_scope.endpoint_id)
        control.check_budget()
        if type(actual.thread_id) is not str or actual.thread_id != record_scope.thread_id or type(actual.endpoint_id) is not str or actual.endpoint_id != record_scope.endpoint_id:
            _refuse()
        admission = _Admission(self, control, actual.admission, actual.thread_id, actual.endpoint_id)
        return admission, _Guard(actual.create_guard, admission, "run_create"), _Guard(actual.read_guard, admission, "source_read")

    def _record_link(self, record, link):
        if type(record) is not PublicationRecord or type(link) is not BotLink or _scope(link, thread_id=record.scope.thread_id, endpoint_id=self._http_owner.endpoint_id, topic=record.scope.message_thread_id) != record.scope:
            _refuse()

    async def _thread_proof(self, link, thread_id, admission):
        response = await self._http_owner.request_json(
            operation="thread_read",
            owner_user_id=link.owner_user_id,
            method="GET",
            path="threads/" + quote(thread_id, safe=""),
            headers={"X-DeerFlow-Require-Thread-Binding": "true"},
            before_dispatch=lambda: admission.authorize("thread_read"),
            after_response=lambda: admission.authorize("thread_read"),
        )
        value = response.body
        if response.status_code != 200 or type(value) is not dict or value.get("thread_id") != thread_id or value.get("owner_user_id") != link.owner_user_id or "agent_instance" not in value or value["agent_instance"] is not None:
            _refuse()
        metadata = value.get("metadata")
        expected = link.agent_ref if link.agent_kind == "custom_agent" else None
        if type(metadata) is not dict or metadata.get("channel_agent_name") != link.agent_ref or metadata.get("agent_name") != expected:
            _refuse()
        admission.check("thread_read")

    async def dispatch(self, link: BotLink, origins: Sequence[InboxRow], *, thread_id: str) -> PublicationRecord:
        try:
            self._check_open(creating=True)
            link, origins = deepcopy(link), tuple(deepcopy(origins))
            origins = _plain_origins(link, origins, thread_id=thread_id, endpoint_id=self._http_owner.endpoint_id)
            scope = _scope(link, thread_id=thread_id, endpoint_id=self._http_owner.endpoint_id, topic=origins[0].message_thread_id)
        except Exception:
            _refuse()

        async def perform(control):
            admission, create_guard, _ = await self._attempt(link, scope, control)
            await admission.authorize("run_create")
            await self._thread_proof(link, thread_id, admission)
            record = await self.inbox.reserve_publication(link, origins, thread_id=thread_id, endpoint_id=scope.endpoint_id, guard=create_guard)
            if type(record) is not PublicationRecord or record.scope != scope:
                _refuse()
            return await self._dispatch_reserved(link, record, origins, admission, create_guard)

        return await self._call(perform, creating=True)

    async def _dispatch_reserved(self, link, record, origins, admission, create_guard, *, saved_files=None):
        if record.dispatch_started:
            return record
        execution_scope = _channel_scope(record)
        facade = self._facade(record, admission)
        parts = [chat_input_parts(row, bot_id=link.bot_id) for row in origins]
        text = "\n\n".join(value for value, _files in parts if value)
        files = [file for _text, items in parts for file in items]
        human_kwargs = {}
        if record.owner_callback_id is not None or any("submission" in row.route_basis for row in origins):
            text, human_kwargs, files = await self.inbox.read_publication_message(record, guard=create_guard)
        message = InboundMessage(
            channel_name="telegram",
            chat_id=str(link.chat_id),
            user_id=str(link.telegram_user_id),
            text=text,
            msg_type=InboundMessageType.CHAT,
            thread_ts=str(origins[0].message_id),
            topic_id=str(record.scope.message_thread_id) if record.scope.message_thread_id is not None else None,
            owner_user_id=link.owner_user_id,
            connection_id=link.connection_id,
            workspace_id=str(link.bot_id),
            metadata={
                "telegram_bot_id": link.bot_id,
                "workspace_id": str(link.bot_id),
                "message_id": origins[0].message_id,
                "event_id": record.event_id,
                "channel_turn_id": record.publication_id,
                "human_message_kwargs": {**human_kwargs, "channel_turn_id": record.publication_id},
            },
            files=files,
            execution_required=True,
            execution_context=ChannelExecutionContext(execution_scope, facade, admission, saved_files={} if saved_files is None else saved_files),
        )
        if files and self.file_preflight is not None:
            allowed = await self.file_preflight(link, record, [dict(file) for file in files], execution_context=message.execution_context)
            await admission.authorize("run_create")
            if type(allowed) is not list or any(type(file) is not dict for file in allowed):
                _refuse()
            allowed = [validate_chat_file(file) for file in allowed]
            remaining = list(files)
            for file in allowed:
                if file not in remaining:
                    _refuse()
                remaining = remaining[remaining.index(file) + 1 :]
            # The original accepted input remains immutable; only this actual
            # transport can omit a refused attachment before its creating mark.
            message.files = allowed
        await admission.authorize("run_create")
        reservation = self.bus.reserve_inbound(message)
        try:
            won = await self.inbox.mark_dispatch_started(record, guard=create_guard)
            if won is True:
                # Keep this fence and actual enqueue synchronous: no await.
                admission.check("run_create")
                reservation.commit(message)
                self._queued[record.publication_id] = message.execution_context
                if record.owner_callback_id is not None:
                    await self.inbox.confirm_callback_dispatch(replace(record, dispatch_started=True), guard=create_guard)
            elif won is not False:
                _refuse()
            return replace(record, dispatch_started=True)
        finally:
            reservation.release()

    async def dispatch_record(self, record: PublicationRecord, current_link: BotLink, *, saved_files: dict[str, dict] | None = None) -> PublicationRecord:
        self._check_open(creating=True)
        record, current_link = deepcopy(record), deepcopy(current_link)
        self._record_link(record, current_link)

        async def perform(control):
            admission, create_guard, _ = await self._attempt(current_link, record.scope, control)
            await admission.authorize("run_create")
            await self._thread_proof(current_link, record.scope.thread_id, admission)
            fresh, origins = await self.inbox.read_publication_input(record, guard=create_guard)
            origins = _plain_origins(current_link, origins, thread_id=record.scope.thread_id, endpoint_id=record.scope.endpoint_id)
            return await self._dispatch_reserved(current_link, fresh, origins, admission, create_guard, saved_files=saved_files)

        return await self._call(perform, creating=True)

    async def retry(self, previous: PublicationRecord, current_link: BotLink, *, on_reserved: Callable[[PublicationRecord], None] | None = None, saved_files: dict[str, dict] | None = None) -> PublicationRecord:
        return await self._retry(previous, current_link, refusal=None, on_reserved=on_reserved, saved_files=saved_files)

    async def retry_refused(self, previous: PublicationRecord, current_link: BotLink, refusal, *, on_reserved: Callable[[PublicationRecord], None] | None = None, saved_files: dict[str, dict] | None = None) -> PublicationRecord:
        from app.channels.outcomes import validate_creation_refusal

        validate_creation_refusal(refusal, channel_execution_scope(previous))
        return await self._retry(previous, current_link, refusal=refusal, on_reserved=on_reserved, saved_files=saved_files)

    async def _retry(self, previous, current_link, *, refusal, on_reserved, saved_files):
        self._check_open(creating=True)
        if on_reserved is not None and not callable(on_reserved):
            _refuse()
        previous, current_link = deepcopy(previous), deepcopy(current_link)
        self._record_link(previous, current_link)

        async def perform(control):
            admission, create_guard, read_guard = await self._attempt(current_link, previous.scope, control)
            control.final_check = lambda: admission.check("run_create")
            await admission.authorize("run_create")
            await self._thread_proof(current_link, previous.scope.thread_id, admission)
            if refusal is None:
                # Read the original predecessor even if its immediate successor
                # committed before a caller lost the result. SQL owns replay CAS.
                facade = self._facade(previous, admission)
                run = await facade.get_run(previous.run_id) if previous.run_id is not None else await facade.lookup_run()
                if run is None or not self._matches(run, previous) or run.row.get("metadata", {}).get("channel_turn_id") != previous.publication_id:
                    _refuse()
                # Binding is only needed for a still-current original receipt.
                # The successor writer independently checks the stored run ID.
                if previous.run_id is None:
                    try:
                        await self.inbox.bind_publication_run(previous, run_id=run.row["run_id"], guard=read_guard)
                    except ValueError:
                        pass
                successor = await self.inbox.reserve_retry_publication(previous, terminal=run, guard=create_guard)
            else:
                successor = await self.inbox.reserve_refused_publication(previous, creation_refusal=refusal, guard=create_guard)
            if on_reserved is not None:
                # Original host bookkeeping only: pin the actual owner before
                # marker/enqueue. This callback grants no dispatch authority.
                result = on_reserved(successor)
                if result is not None:
                    if asyncio.iscoroutine(result):
                        result.close()
                    _refuse()
            fresh, origins = await self.inbox.read_publication_input(successor, guard=create_guard)
            return await self._dispatch_reserved(current_link, fresh, _plain_origins(current_link, origins, thread_id=fresh.scope.thread_id, endpoint_id=fresh.scope.endpoint_id), admission, create_guard, saved_files=saved_files)

        return await self._call(perform, creating=True)

    def _facade(self, record, admission):
        if (admission.thread_id, admission.endpoint_id) != (record.scope.thread_id, record.scope.endpoint_id):
            _refuse()
        return ChannelExecutionFacade(self._http_owner, scope=_channel_scope(record), admission=admission, capture_output=True, capture_inventory=True, inventory_document_version=2)

    @staticmethod
    def _matches(run, record):
        proof, scope = run.receipt, record.scope
        return proof is not None and (proof.owner_user_id, proof.thread_id, proof.publication_id, proof.assistant_id, proof.agent_name) == (scope.owner_user_id, scope.thread_id, record.publication_id, scope.assistant_id, scope.agent_name)

    async def read_output(self, record: PublicationRecord, current_link: BotLink) -> RunOutputProjectionV2 | None:
        try:
            self._check_open(creating=False)
            record, current_link = deepcopy(record), deepcopy(current_link)
            self._record_link(record, current_link)
            if record.run_id is None:
                _refuse()
        except Exception:
            _refuse()

        async def perform(control):
            admission, _, _ = await self._attempt(current_link, record.scope, control)
            control.final_check = lambda: admission.check("source_read")
            result = await self._facade(record, admission).get_run_output(record.run_id)
            admission.check("source_read")
            if result is not None and type(result) is not RunOutputProjectionV2:
                _refuse()
            return result

        return await self._call(perform, timeout=OUTPUT_READ_TIMEOUT_SECONDS)

    async def _locate(self, record, admission, read_guard, facade, max_pages, *, metadata):
        def matches(run):
            saved = run.row.get("metadata")
            receipt_match = self._matches(run, record)
            marker_match = isinstance(saved, Mapping) and saved.get("channel_turn_id") == record.publication_id
            if metadata and receipt_match != marker_match:
                _refuse()
            return receipt_match and (not metadata or marker_match)

        if record.run_id is not None:
            run = await facade.get_run(record.run_id)
            if run is None or not matches(run):
                return PublicationRunObservation("held", record, reason="source_unavailable")
            return PublicationRunObservation("matched", record, run)
        if metadata:
            match = await facade.lookup_run()
            if match is None:
                return PublicationRunObservation("pending", record, reason="awaiting_run")
            if not matches(match):
                _refuse()
            admission.check("source_read")
            fresh = await self.inbox.bind_publication_run(record, run_id=match.row["run_id"], guard=read_guard)
            return PublicationRunObservation("matched", fresh, match)
        cursor, match, seen = None, None, set()
        for _ in range(max_pages):
            page = await facade.list_runs_page(limit=OBSERVATION_PAGE_SIZE, before=cursor)
            for run in page.data:
                identity = run.row["run_id"]
                if identity in seen:
                    _refuse()
                seen.add(identity)
                if matches(run):
                    if match is not None:
                        return PublicationRunObservation("held", record, reason="ambiguous_run")
                    match = run
            if not page.has_more:
                break
            cursor = page.next_before
        else:
            return PublicationRunObservation("budget_exhausted", record, reason="budget_exhausted")
        if match is None:
            return PublicationRunObservation("pending", record, reason="awaiting_run")
        admission.check("source_read")
        fresh = await self.inbox.bind_publication_run(record, run_id=match.row["run_id"], guard=read_guard)
        return PublicationRunObservation("matched", fresh, match)

    async def locate_run(self, record: PublicationRecord, current_link: BotLink, *, max_pages: int, deadline: float | None = None) -> PublicationRunObservation:
        self._record_link(record, current_link)
        if type(max_pages) is not int or not 1 <= max_pages <= MAX_OBSERVATION_PAGES or deadline is not None and (type(deadline) not in {int, float} or not math.isfinite(deadline)):
            _refuse()
        record, current_link = deepcopy(record), deepcopy(current_link)

        async def perform(control):
            try:
                admission, _, guard = await self._attempt(current_link, record.scope, control)
                control.final_check = lambda: admission.check("source_read")
                return await self._locate(record, admission, guard, self._facade(record, admission), max_pages, metadata=True)
            except (asyncio.CancelledError, _DeadlineExceeded):
                raise
            except Exception:
                return PublicationRunObservation("held", record, reason="source_unavailable")

        return await self._call(perform, timeout=OBSERVATION_TIMEOUT_SECONDS, expired=lambda: PublicationRunObservation("budget_exhausted", record, reason="budget_exhausted"), deadline=deadline)

    async def _close_run(self, record, link, run, guard, *, answered):
        """A finished run that cannot be delivered ends in what it was: its answer on the web, or a way to try again."""
        from app.telegram_bots.errors import classify_run

        failure = classify_run(run, channel_execution_scope(record))
        on_web = failure is None and answered
        key = "answer.on_web" if on_web else "error.generic" if failure is None else "busy.gave_up" if failure.retry_later else failure.text_key
        source = await fixed_source(self.host, link, key)
        identity = await self.outbox.plan_publication_outcome(
            record, run=run, source_ref=source, keyboard=() if on_web else _try_again(record), callbacks=self.host.callbacks, guard=guard, undeliverable=failure is None, url_button=await conversation_button(link, record) if on_web else None
        )
        return PublicationObservation("planned", record, outbox_id=identity)

    async def observe(self, record: PublicationRecord, current_link: BotLink, *, max_pages: int, deadline: float | None = None, closing: str | None = None) -> PublicationObservation:
        """Plan what a saved publication can still say. With `closing`, nobody else will: end it.

        `closing` is the sentence for a message that found no run. Only a caller that knows no
        live turn owns this publication may pass it.
        """
        try:
            self._check_open(creating=False)
            if type(max_pages) is not int or not 1 <= max_pages <= MAX_OBSERVATION_PAGES:
                _refuse()
            if deadline is not None and (type(deadline) not in {int, float} or not math.isfinite(deadline)):
                _refuse()
            record, current_link = deepcopy(record), deepcopy(current_link)
            self._record_link(record, current_link)
        except Exception:
            _refuse()

        async def perform(control):
            try:
                if closing is not None:
                    self.fence(record)
                admission, _, read_guard = await self._attempt(current_link, record.scope, control)
                control.final_check = lambda: admission.check("source_read")
                facade = self._facade(record, admission)
                located = await self._locate(record, admission, read_guard, facade, max_pages, metadata=False)
                if located.state == "pending" and closing is not None and self.worker_state(record) != "taken":
                    keyboard = () if closing == "busy.cancelled" else _try_again(located.record)
                    identity = await self.outbox.plan_publication_outcome(located.record, no_run=True, source_ref={"key": closing, "values": {}}, keyboard=keyboard, callbacks=self.host.callbacks, guard=read_guard)
                    return PublicationObservation("planned", located.record, outbox_id=identity)
                if located.state != "matched":
                    return PublicationObservation(located.state, located.record, reason=located.reason)
                fresh = located.record

                async def unfinished(reason):
                    if closing is None:
                        return PublicationObservation("held", fresh, reason=reason)
                    return await self._close_run(fresh, current_link, located.run, read_guard, answered=output.output.state == "complete")

                output = await facade.get_run_output(fresh.run_id)
                admission.check("source_read")
                if output is None:
                    return PublicationObservation("held", fresh, reason="source_unavailable")
                if output.output.state == "pending":
                    return PublicationObservation("pending", fresh, reason="awaiting_output")
                inventory = output.inventory
                if self.human_input_planner is not None and output.output.state == "unavailable" and output.output.reason == "human_input_required" and inventory.state == "matched" and inventory.document.coverage == "complete":
                    control.check_budget()
                    admission.check("source_read")
                    identity = await self.human_input_planner(fresh, current_link, guard=read_guard)
                    control.check_budget()
                    admission.check("source_read")
                    if identity is not None:
                        from app.telegram_bots.inbox import publication_identity
                        from app.telegram_bots.model_source import validate_model_reply_basis

                        item = await self.outbox.get(current_link, identity)
                        proof = await await_drained(asyncio.to_thread(validate_model_reply_basis, item))
                        if proof.kind not in {"human_input", "human_input_files"} or proof.output != output or publication_identity(proof.record) != publication_identity(fresh) or item.payload["reply_basis"]["finish_route"] is not True:
                            _refuse()
                        return PublicationObservation("planned", fresh, outbox_id=identity)
                if self.file_output_planner is not None and inventory.state == "matched" and inventory.document.coverage == "complete" and delivery_items(inventory.document):
                    identity = await self.file_output_planner(fresh, current_link, output, guard=read_guard)
                    control.check_budget()
                    admission.check("source_read")
                    if identity is not None:
                        from app.telegram_bots.inbox import publication_identity
                        from app.telegram_bots.model_source import validate_model_reply_basis

                        item = await self.outbox.get(current_link, identity)
                        proof = await await_drained(asyncio.to_thread(validate_model_reply_basis, item))
                        if proof.kind != "file_outputs" or proof.output != output or publication_identity(proof.record) != publication_identity(fresh):
                            _refuse()
                        return PublicationObservation("planned", fresh, outbox_id=identity)
                if inventory.state != "matched" or inventory.document.coverage != "complete" or delivery_items(inventory.document):
                    return await unfinished("inventory_unhandled")
                if output.output.state != "complete":
                    return await unfinished("source_unavailable")
                try:
                    control.check_budget()
                    await await_drained(asyncio.to_thread(freeze_reply_presentation, output.output.source.text, text_sha256=output.output.output_evidence.text_sha256))
                    admission.check("source_read")
                    identity = await self.outbox.plan_verified_publication_reply(fresh, output=output, guard=read_guard)
                except ValueError:
                    return await unfinished("unsupported_presentation")
                return PublicationObservation("planned", fresh, outbox_id=identity)
            except asyncio.CancelledError:
                raise
            except _DeadlineExceeded:
                raise
            except Exception:
                return PublicationObservation("held", record, reason="source_unavailable")

        return await self._call(perform, timeout=OBSERVATION_TIMEOUT_SECONDS, expired=lambda: PublicationObservation("budget_exhausted", record, reason="budget_exhausted"), deadline=deadline)

    def begin_shutdown(self) -> None:
        self._creating = False

    async def _finish_stop(self):
        self._creating = self._reading = False
        while self._operations:
            pending = tuple(self._operations)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

    async def stop(self) -> None:
        if asyncio.current_task() in self._operations:
            _refuse()
        self._creating = self._reading = False
        if self._stop_task is None:
            self._stop_task = asyncio.create_task(self._finish_stop(), name="telegram-publication-stop")
        await await_drained(asyncio.shield(self._stop_task))
