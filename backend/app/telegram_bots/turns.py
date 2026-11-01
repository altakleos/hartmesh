"""Owned chat turns; durable input precedes ephemeral queue and execution work."""

from __future__ import annotations

import asyncio
import logging
import secrets
import weakref
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime

from app.channels.message_bus import InboundMessage
from app.telegram_bots.queue import ChatQueue
from deerflow.runtime.keyed_lock import AsyncKeyedLockTable
from deerflow.runtime.runs.terminal import failure_chain
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)
TURN_WAIT_REPORT_SECONDS = 600
RUN_SHOWS_SECONDS = 5
LEFT_INPUTS_SECONDS = 30
_RUN_ENDED = frozenset({"success", "error", "timeout", "interrupted"})
_SOURCES = "telegram_source_message_ids"
_ALL_TOPICS = object()


class _TurnStalled(Exception):
    """A turn showed no life for longer than it may hold its chat."""


def _chat_line(event, *, chat=None, error=None, **facts):
    """One line an operator can act on: whose chat, and why it stopped."""
    known = {"link": chat.id if chat is not None else None, "bot": chat.bot_id if chat is not None else None, **facts, "cause": failure_chain(error) or None}
    logger.warning("telegram_bots %s %s", event, " ".join(f"{key}={value}" for key, value in known.items() if value is not None))


def forward_origin(message):
    """Whether an actual message is forwarded, and the name it came from."""
    forwarded = message.forward_origin is not None or message.is_automatic_forward is True or any(message.api_kwargs.get(key) is not None for key in ("forward_from", "forward_from_chat", "forward_sender_name", "forward_date"))
    if not forwarded:
        return False, None
    origin = message.forward_origin
    author = getattr(origin, "sender_user", None)
    chat = getattr(origin, "sender_chat", None) or getattr(origin, "chat", None)
    return True, getattr(author, "first_name", None) or getattr(origin, "sender_user_name", None) or getattr(chat, "title", None) or "an unnamed sender"


class _Guard:
    """Original host lifetime around the existing borrowed SQL custody guard."""

    def __init__(self, services, channel, link, current, *, repair=False, borrowed=None):
        self.services, self.channel, self.captured_link = services, channel, link
        self._current, self.repair, self.actual = current, repair, borrowed
        self.borrowed = borrowed

    def current(self):
        if self._current() is not True:
            raise ValueError("chat-turn-retired") from None

    async def prepare(self):
        self.current()
        if self.borrowed is not None:
            prepare = getattr(self.borrowed, "prepare", None)
            if prepare is not None:
                await prepare()
            self.current()
            return
        if not self.repair:
            await self.services.prepare_ingress_values(self.captured_link)
            self.current()
        owner = await self.services.prepare_live_owner(self.captured_link.owner_user_id)
        self.current()
        self.actual = self.services.links.publication_guard(self.captured_link, check_live_owner=owner, check_attempt_current=self._current)

    async def check(self, writer):
        self.current()
        if self.actual is None:
            raise ValueError("chat-turn-unprepared") from None
        result = await self.actual.check(writer)
        self.current()
        return result


@dataclass(eq=False)
class _Turn:
    channel: object
    link: object
    inbound: InboundMessage
    origins: tuple
    record: object = None
    done: asyncio.Event = field(default_factory=asyncio.Event)
    wake: asyncio.Event = field(default_factory=asyncio.Event)
    refusal: object = None
    stop_requested: bool = False
    waiting_since: float | None = None
    active: bool = True
    task: asyncio.Task | None = None
    presentation: object = None
    stop_id: str | None = None
    stop_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    callback_id: str | None = None
    retry_at: float | None = None
    cancelled_waiting: tuple = ()
    stop_commands: list = field(default_factory=list)
    stop_status: str | None = None
    input_closed: bool = False
    cancel_future: bool = False
    waiting_dropped: bool = False
    capacity_waiting: bool = False
    capacity_notice_sent: bool = False
    stop_marker_checked: bool = False
    stop_press: tuple | None = None
    waiting_told: bool = False
    submission: object = None
    quiet_since: float | None = None
    observed_since: float | None = None
    wait_reported: bool = False
    run_id: str | None = None
    closing: bool = False
    error_plan_refused: bool = False
    saved_files: dict = field(default_factory=dict)

    @property
    def topic(self):
        return self.origins[0].message_thread_id

    def held(self, event, error=None, **facts):
        """`record.run_id` stays empty on a live turn: the observer binds the run to its own copy."""
        _chat_line(event, chat=self.link, error=error, turn=self.record.publication_id if self.record is not None else None, run=self.run_id, **facts)


@dataclass(frozen=True)
class FileInputContext:
    """Finite captured view issued from one actual active turn."""

    link: object
    record: object
    origins: tuple
    _file: dict
    text: str
    _files: list
    _turn: object = field(repr=False, compare=False)
    _is_current: object = field(repr=False, compare=False)

    def current(self):
        return self._is_current() is True

    @property
    def file(self):
        return deepcopy(self._file)

    @property
    def files(self):
        return deepcopy(self._files)


@dataclass(eq=False)
class _JoinedInput:
    channel: object
    link: object
    origins: list
    kind: str
    basis: tuple
    task: asyncio.Task | None = None


class ChatTurns:
    def __init__(self, services):
        self.services = services
        self.queue = ChatQueue(services.config)
        self._turns, self._origins = {}, {}
        self._callback_origins = {}
        self._tasks = set()
        self._task_channels = {}
        self._entries = {}
        self._joins = {}
        self._join_locks = AsyncKeyedLockTable()
        self._join_busy = {}
        self._retired = weakref.WeakSet()
        self._retire_tasks = {}
        self._origin_channels = {}
        self._closed = False
        self._observer = None
        self._stop_task = None
        self._started_at = datetime.now(UTC)

    def _current(self, turn):
        channel, link = turn.channel, turn.link
        read_allowed = getattr(self.services, "publication_read_allowed", None)
        return (
            not self._closed
            and turn.active
            and not turn.done.is_set()
            and channel not in self._retired
            and self._turns.get((link.id, turn.topic)) is turn
            and self.services.channel(link.id) is channel
            and channel._same_scope(link, channel.link)
            and channel.transport_revision == link.token_revision
            and channel.application.running
            and not channel._stopping
            and (read_allowed is None or read_allowed() is True)
        )

    def _creating(self, turn):
        return self._current(turn) and turn.channel._callback_current(turn.link)

    def _open(self, channel, link):
        return not self._closed and channel not in self._retired and channel._callback_current(link)

    @asynccontextmanager
    async def _entry(self, channel):
        task = asyncio.current_task()
        self._entries[task] = channel
        try:
            yield
        finally:
            self._entries.pop(task, None)

    def _publication_current(self, record):
        turn = self._turns.get((record.scope.link_id, record.scope.message_thread_id))
        return turn is not None and self._current(turn) and turn.record is not None and turn.record.publication_id == record.publication_id

    @property
    def observer(self):
        if self._observer is None:
            from app.telegram_bots.progress import RunObserver

            self._observer = RunObserver(coordinator=self.services.publications, http_owner=self.services.thread_client, capture_attempt=self.services.publication_authority.capture, is_current=self._publication_current)
        return self._observer

    def _spawn(self, coroutine, *, channel, turn=None):
        task = asyncio.create_task(coroutine, name="telegram-chat-turn")
        self._tasks.add(task)
        self._task_channels[task] = channel

        def finished(done):
            self._tasks.discard(done)
            self._task_channels.pop(done, None)
            if not done.cancelled() and done.exception() is not None:
                if turn is not None:
                    turn.held("chat_turn_held", done.exception())
                else:
                    _chat_line("chat_turn_held", chat=channel.link, error=done.exception())

        task.add_done_callback(finished)
        return task

    def _file_lifetime(self, turn, record, execution_context):
        from app.channels.execution import ChannelExecutionContext
        from app.telegram_bots.publication import channel_execution_scope

        if not self._current(turn) or turn.record is None or turn.record.publication_id != record.publication_id or turn.record.scope != record.scope:
            return False
        if execution_context is None:
            return True
        return type(execution_context) is ChannelExecutionContext and execution_context.scope == channel_execution_scope(record) and execution_context.admission.current("run_create") is True

    async def file_context(self, channel, file, *, execution_context=None):
        """Resolve exact saved descriptor membership without a second registry."""
        from app.telegram_bots.inbox import chat_input_parts, validate_chat_file

        try:
            file = validate_chat_file(file)
        except ValueError:
            return None
        candidates = [
            turn
            for turn in self._turns.values()
            if turn.channel is channel and turn.record is not None and self._file_lifetime(turn, turn.record, execution_context) and any(file == item for row in turn.origins for item in row.sanitized_input.get("files", []))
        ]
        if len(candidates) != 1:
            return None
        turn = candidates[0]
        original = turn.record

        def current():
            return self._file_lifetime(turn, original, execution_context)

        guard = _Guard(self.services, channel, turn.link, current, repair=True)
        fresh, origins = await self.services.outbox.inbox.read_publication_input(original, guard=guard)
        guard.current()
        parts = [chat_input_parts(row, bot_id=turn.link.bot_id) for row in origins]
        files = [item for _text, members in parts for item in members]
        if file not in files or fresh.publication_id != turn.record.publication_id:
            return None
        text = "\n\n".join(value for value, _files in parts if value)
        if fresh.owner_callback_id is not None or any("submission" in row.route_basis for row in origins):
            text, _kwargs, files = await self.services.outbox.inbox.read_publication_message(fresh, guard=guard)
        if file not in files:
            return None
        guard.current()
        return FileInputContext(deepcopy(turn.link), fresh, origins, deepcopy(file), text, files, turn, current)

    async def file_notice(self, channel, context, *, key, values, url_button=None, retry=False, execution_context=None):
        if type(context) is not FileInputContext or context._turn.channel is not channel or not context.current() or context._turn.record is None or context._turn.record.publication_id != context.record.publication_id:
            return None

        def current():
            return context.current() and self._file_lifetime(context._turn, context.record, execution_context)

        if not current():
            return None
        guard = _Guard(self.services, channel, context.link, current, repair=True)
        return await self.services.outbox.plan_inbound_file_notice(context.record, context.file, source_ref={"key": key, "values": values}, callbacks=self.services.callbacks, guard=guard, retry=retry, url_button=url_button)

    async def accept(self, channel, update, *, separate=False):
        """``separate`` is one indivisible input: a skill's word joins nothing."""
        async with self._entry(channel):
            return await self._accept(channel, update, separate=separate)

    def unfinished(self, link, row):
        """Whether a saved message's admission still has to happen, so that its replay goes on with it.

        Nothing holds the row and no turn took it. A routed row from before this process belongs
        to recovery: an ordinary message cut off by a restart does not start again by itself.
        A row with no route never started anything, and recovery cannot read it.
        """
        key = (link.id, row.message_thread_id)
        joined, turn = self._joins.get(key), self._turns.get(key)
        kept = (*(joined.origins if joined is not None else ()), *(turn.stop_commands if turn is not None else ()))
        held = (link.id, row.message_id) in self._origins or any(saved.message_id == row.message_id for saved in kept)
        return not held and not row.route_complete and row.turn_id is None and row.outbox_id is None and (row.route_kind is None or row.accepted_at >= self._started_at)

    async def accept_staged(self, channel, stage):
        """Admit one actual durable answer as an indivisible FIFO input."""
        async with self._entry(channel):
            link = deepcopy(channel.link)

            def current():
                return not self._closed and channel not in self._retired and self.services.channel(link.id) is channel and channel._callback_current(link)

            guard = _Guard(self.services, channel, link, current, repair=True)
            if not current():
                return False
            stage = await self.services.outbox.inbox.read_submission(link, stage, guard=guard)
            guard.current()
            if stage.route_complete or stage.record is not None:
                return True
            if stage.callback_id is not None:
                if any(turn.callback_id == stage.callback_id for turn in self._turns.values()) or any(entry.callback_id == stage.callback_id for entry in self.queue.waiting_inputs(link, stage.origins[0].message_thread_id)):
                    return True
            elif any((link.id, row.message_id) in self._origins for row in stage.origins):
                return True
            inbound = replace(self._inbound(link, stage.origins), text=stage.text)
            result = self.queue.admit_callback(link, inbound, callback_id=stage.callback_id) if stage.callback_id is not None else self.queue.admit_submission(link, inbound)
            if result == "full":
                await self.services.outbox.plan_input_outcome(
                    link,
                    source_id="submission-full:" + (stage.callback_id or str(stage.origins[0].message_id)),
                    source_ref={"key": "busy.followup_full", "values": {}},
                    keyboard=self._button("retry_message", stage.callback_id or str(stage.origins[0].message_id), target_kind="request"),
                    callbacks=self.services.callbacks,
                    guard=guard,
                    inbox_rows=() if stage.callback_id else stage.origins,
                    input_owner_callback_id=stage.callback_id,
                )
                return True
            if stage.callback_id is not None:
                self._callback_origins[(link.id, stage.callback_id)] = (channel, stage.origins)
            else:
                for row in stage.origins:
                    self._origins[(link.id, row.message_id)] = row
                    self._origin_channels[(link.id, row.message_id)] = channel
            if result == "run":
                self._start(channel, link, inbound)
            return True

    async def _accept(self, channel, update, *, separate=False):
        link, message = deepcopy(channel._admission(update).link), update.message
        if self._closed or channel in self._retired or message is None:
            return False
        files = channel._extract_inbound_files(message)
        files = [{**value, "message_id": message.message_id, "telegram_bot_id": link.bot_id} for value in files]
        text = message.text if type(message.text) is str else message.caption if type(message.caption) is str else ""
        if separate:
            text = channel.command_text(message)  # A skill's word reaches the run as the person meant it.
        if not text.strip() and not files:
            return False
        if not await channel._input_current(link) or self._closed or channel in self._retired or not channel._callback_current(link):
            return False
        value = {"type": "media" if files else "text", "text": text, **({"files": files} if files else {})}
        row, created = await await_drained(self.services.outbox.inbox.accept(link, update, sanitized_input=value))
        if not created and not self.unfinished(link, row):
            channel._classified(update, remember=True)
            return True
        # From here a channel going away leaves the update unacknowledged, so the row is not stranded.
        if not self._open(channel, link):
            return False
        forwarded, name = forward_origin(message)
        profile = {"version": 2, "kind": "chat_input", "forwarded": forwarded, "origin": name} if files or forwarded else {"version": 1, "kind": "plain_text", "forwarded": False}
        row = await await_drained(self.services.outbox.inbox.route(row, kind="turn", target_id=link.agent_ref, basis={"input_profile": profile}, operation_ids=[]))
        if not self._open(channel, link):
            return False
        key = (link.id, row.message_thread_id)
        async with self._join_locks.hold(key):
            self._join_busy[key] = channel
            try:
                await self._sequence_input(channel, link, row, files, forwarded, text, separate=separate)
            finally:
                self._join_busy.pop(key, None)
        channel._classified(update, remember=True)
        return True

    async def accept_transcript(self, channel, link, row):
        """A finished voice note takes the path of the same typed text."""
        async with self._entry(channel):
            if not self._open(channel, link):
                return False
            human_inputs = getattr(self.services, "human_inputs", None)
            if human_inputs is not None and await human_inputs.handle_transcript(channel, link, row):
                return True
            key = (link.id, row.message_thread_id)
            async with self._join_locks.hold(key):
                self._join_busy[key] = channel
                try:
                    await self._sequence_input(channel, link, row, [], row.route_basis["input_profile"]["forwarded"], row.sanitized_input["text"])
                finally:
                    self._join_busy.pop(key, None)
            return True

    async def _sequence_input(self, channel, link, row, files, forwarded, text, *, separate=False):
        from app.telegram_bots.inbox import _scope

        key = (link.id, row.message_thread_id)
        basis = _scope(row)
        album = row.sanitized_input.get("media_group_id")
        kind = "album" if album is not None else "forward" if forwarded else "longtext" if len(text.encode("utf-16-le")) // 2 >= 4000 else None
        basis = (*basis, album if kind == "album" else None)
        prior = self._joins.get(key)
        compatible = not separate and prior is not None and prior.channel is channel and prior.basis == basis and (prior.kind == kind or prior.kind == "longtext" and kind is None) and len(prior.origins) < 64
        if prior is not None and not compatible:
            await self._flush_join_locked(key, prior)
        if not files and not forwarded:
            pending = getattr(self.services, "pending_files", None)
            if pending is not None:
                guard = _Guard(self.services, channel, link, lambda: self._open(channel, link), repair=True)
                await pending.take(link, row, guard=guard)
                await guard.prepare()
                async with self.services.outbox.inbox._transaction() as writer:
                    row, _taken = await self.services.outbox.inbox.pending_file_take(link, row, session=writer, guard=guard)
        if compatible:
            prior.origins.append(row)
            if prior.task is not None:
                prior.task.cancel()
                await asyncio.gather(prior.task, return_exceptions=True)
            self._arm_join(key, prior)
        elif kind is not None and not separate:
            joined = _JoinedInput(channel, link, [row], kind, basis)
            self._joins[key] = joined
            self._arm_join(key, joined)
        else:
            await self._admit_origins(channel, link, (row,), separate=separate)

    def _arm_join(self, key, joined):
        async def wait():
            await asyncio.sleep(1.5 if joined.kind == "longtext" else 1.0)
            await self._flush_join(key, joined)

        joined.task = self._spawn(wait(), channel=joined.channel)

    async def _flush_join(self, key, joined):
        async with self._join_locks.hold(key):
            self._join_busy[key] = joined.channel
            try:
                await self._flush_join_locked(key, joined)
            finally:
                self._join_busy.pop(key, None)

    async def _flush_join_locked(self, key, joined):
        if self._joins.get(key) is not joined:
            return
        self._joins.pop(key)
        if joined.task is not None and joined.task is not asyncio.current_task():
            joined.task.cancel()
            await asyncio.gather(joined.task, return_exceptions=True)
        if self._open(joined.channel, joined.link):
            try:
                await self._admit_origins(joined.channel, joined.link, tuple(joined.origins))
            except asyncio.CancelledError:
                raise
            except Exception as error:
                await self._flush_failed(joined, error)

    async def _flush_failed(self, joined, error):
        """A join's updates were acknowledged when they joined, so no replay will bring these back."""
        channel, link, first = joined.channel, joined.link, joined.origins[0]
        _chat_line("chat_join_held", chat=link, error=error)
        if (link.id, first.message_id) in self._origins:
            return  # In line, and only its notice failed.
        try:
            from app.telegram_bots.publication import fixed_source

            await self.services.outbox.plan_input_outcome(
                link,
                source_id=f"chat-input:{link.chat_id}:{link.binding_generation}:{first.message_id}",
                source_ref=await fixed_source(self.services, link, "error.generic"),
                keyboard=self._button("retry_message", str(first.message_id), target_kind="request"),
                callbacks=self.services.callbacks,
                guard=_Guard(self.services, channel, link, lambda: self._open(channel, link), repair=True),
                inbox_rows=tuple(joined.origins),
            )
        except asyncio.CancelledError:
            raise
        except Exception as failure:
            # Unanswered for now. The next start of this channel or process closes it.
            _chat_line("chat_join_held", chat=link, error=failure)

    async def flush_inputs(self, channel, *, topic=_ALL_TOPICS):
        """Flush earlier admitted pieces before an incompatible navigation input."""
        keys = set(self._joins) | set(self._join_busy) if topic is _ALL_TOPICS else {(channel.link.id, topic)}
        for key in sorted(keys, key=lambda value: (value[0], value[1] or 0)):
            async with self._join_locks.hold(key):
                joined = self._joins.get(key)
                if joined is not None and joined.channel is channel:
                    self._join_busy[key] = channel
                    try:
                        await self._flush_join_locked(key, joined)
                    finally:
                        self._join_busy.pop(key, None)

    async def _admit_origins(self, channel, link, origins, *, separate=False):
        inbound = self._inbound(link, origins)
        if not inbound.text.strip() and inbound.files:
            pending = getattr(self.services, "pending_files", None)
            if pending is None:
                raise ValueError("pending-files-unavailable") from None
            guard = _Guard(self.services, channel, link, lambda: self._open(channel, link), repair=True)
            saved = []
            for row in origins:
                for file in row.sanitized_input.get("files", []):
                    admit = getattr(channel, "admit_file", None)
                    allowed = True if admit is None else await admit(link, row, file)
                    guard.current()
                    if allowed:
                        await pending.add(link, row, file, guard=guard)
                        saved.append((row, file))
                    else:
                        await guard.prepare()
                        async with self.services.outbox.inbox._transaction() as writer:
                            await self.services.outbox.inbox.finish_route(row, session=writer)
                            await guard.check(writer)
            if saved:
                # An album is one message, so it gets one question. A photo's own name is made up, so it is not shown.
                row, file = saved[0]
                key, values = ("file.saved_ask_many", {"n": len(saved)}) if len(saved) > 1 else ("file.saved_ask_photo", {}) if file["type"] == "image" else ("file.saved_ask_plain", {"file": file["filename"]})
                rows = [saved_row for saved_row, _file in saved]
                await await_drained(channel.send_text(key, source_id=f"file-received:{row.chat_id}:{row.message_id}", reply_to=row.message_id, thread=row.message_thread_id, inbox_rows=rows, finish_route=True, _captured_link=link, **values))
            return
        disposition = self.queue.admit_submission(link, inbound) if separate or "reply_to_message_id" in origins[0].sanitized_input else self.queue.admit(link, inbound)
        if disposition == "full":
            first = origins[0]
            await await_drained(
                channel.send_text("busy.followup_full", source_id=f"chat-full:{first.chat_id}:{first.message_id}", reply_to=first.message_id, thread=first.message_thread_id, inbox_rows=origins, finish_route=True, _captured_link=link)
            )
            return
        for row in origins:
            self._origins[(link.id, row.message_id)] = row
            self._origin_channels[(link.id, row.message_id)] = channel
        if disposition == "run":
            self._start(channel, link, inbound)
        elif disposition == "first-waiting":
            first = origins[0]
            await await_drained(channel.send_text("busy.followup", source_id=f"chat-wait:{first.chat_id}:{first.message_id}", reply_to=first.message_id, thread=first.message_thread_id, inbox_rows=origins, _captured_link=link))
        else:
            self._spawn(channel.react_waiting(link, origins[0]), channel=channel)

    def _start(self, channel, link, inbound):
        topic = inbound.metadata.get("message_thread_id")
        callback_id = self.queue.current_callback(link, topic)
        origins = self._callback_origins.pop((link.id, callback_id))[1] if callback_id is not None else tuple(self._origins[(link.id, identity)] for identity in inbound.metadata[_SOURCES])
        turn = _Turn(channel, link, inbound, origins)
        turn.callback_id = callback_id
        if turn.callback_id is None and len(origins) == 1 and "submission" in origins[0].route_basis:
            turn.submission = self.services.outbox.inbox.typed_submission_stage(origins[0])
        self._turns[(link.id, turn.topic)] = turn
        if self._creating(turn):
            turn.task = self._spawn(self._own(turn), channel=channel, turn=turn)

    async def _own(self, turn):
        """Every way out of the driver ends the turn, or it would hold its chat for good."""
        try:
            await self._drive(turn)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            turn.held("chat_turn_held", error)
        key = (turn.link.id, turn.topic)
        if turn.done.is_set() or self._turns.get(key) is not turn:
            return
        if self._current(turn):
            try:
                await self._close(turn)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                # Without a plan the person hears nothing, but the next message can still run.
                turn.held("chat_close_held", error)
        if turn.done.is_set() or self._turns.get(key) is not turn:
            return
        if self._current(turn):
            self._finish(turn)
        else:
            self._forget(turn)

    async def _close(self, turn):
        """The ending of a turn its driver could not finish."""
        from app.telegram_bots.publication import MAX_OBSERVATION_PAGES

        stopping = turn.stop_requested
        key = "busy.cancelled" if stopping else "error.generic"
        turn.closing = True
        if turn.record is None:
            await self._input_outcome(turn, key, keyboard=() if stopping else self._button("retry_message", turn.callback_id or str(turn.origins[0].message_id), target_kind="request"))
        else:
            record = turn.record
            attempt = await self.services.publication_authority.capture(turn.link, thread_id=record.scope.thread_id, endpoint_id=record.scope.endpoint_id)
            guard = self._guard(turn, borrowed=attempt.cancel_guard)
            fresh, _ = await self.services.outbox.inbox.read_publication_input(record, guard=guard)
            stop_id = turn.stop_id or "closed:" + fresh.publication_id
            if not fresh.dispatch_started:
                fresh = await self.services.outbox.inbox.request_publication_stop(fresh, stop_id=stop_id, guard=guard)
            if fresh.dispatch_started:
                # Not planned means a run is still going or could not be read. Recovery owns it from here.
                result = await self.services.publications.observe(fresh, turn.link, max_pages=MAX_OBSERVATION_PAGES, closing=key)
                if result.state != "planned":
                    turn.held("chat_close_held", state=result.state, reason=result.reason)
            else:
                keyboard = () if stopping else self._button("retry_message", fresh.publication_id)
                await self.services.outbox.plan_cancelled_before_dispatch(fresh, stop_id=stop_id, guard=guard, callbacks=self.services.callbacks, keyboard=keyboard, key=key)
        if stopping:
            self._drop_waiting(turn)
        await self._complete(turn)

    def _forget(self, turn):
        """Let go of a turn with no way to answer, as a retired channel does for all of its own."""
        turn.active = False
        turn.done.set()
        if turn.record is not None and self._observer is not None:
            self.observer.retire(turn.record)
        self._let_go(turn.link, [row.message_id for row in turn.origins])
        for entry in self.queue.waiting_inputs(turn.link, turn.topic):
            if entry.callback_id is not None:
                self._callback_origins.pop((turn.link.id, entry.callback_id), None)
            else:
                self._let_go(turn.link, entry.inbound.metadata[_SOURCES])
        self.queue.cancel_wait(turn.link, turn.topic)
        self.queue.turn_ended(turn.link, turn.topic)
        self._turns.pop((turn.link.id, turn.topic), None)

    def _let_go(self, link, message_ids):
        for identity in message_ids:
            self._origins.pop((link.id, identity), None)
            self._origin_channels.pop((link.id, identity), None)

    def _finish(self, turn):
        if not self._current(turn) or turn.done.is_set():
            return
        turn.done.set()
        if turn.record is not None and self._observer is not None:
            self.observer.retire(turn.record)
        if turn.callback_id is None:
            self._let_go(turn.link, [row.message_id for row in turn.origins])
        next_input = self.queue.turn_ended(turn.link, turn.topic)
        self._turns.pop((turn.link.id, turn.topic), None)
        if next_input is not None:
            self._start(turn.channel, turn.link, next_input)

    def _guard(self, turn, *, creating=False, repair=False, borrowed=None):
        current = (lambda: self._creating(turn) and not turn.stop_requested) if creating else lambda: self._current(turn)
        return _Guard(self.services, turn.channel, turn.link, current, repair=repair, borrowed=borrowed)

    @staticmethod
    def _inbound(link, origins):
        from app.telegram_bots.inbox import chat_input_parts

        first = origins[0]
        parts = [chat_input_parts(row, bot_id=link.bot_id) for row in origins]
        return InboundMessage(
            channel_name="telegram",
            chat_id=str(link.chat_id),
            user_id=str(link.telegram_user_id),
            text="\n\n".join(text for text, _files in parts if text),
            files=[file for _text, files in parts for file in files],
            thread_ts=str(first.message_id),
            topic_id=str(first.message_thread_id) if first.message_thread_id is not None else None,
            connection_id=link.connection_id,
            owner_user_id=link.owner_user_id,
            workspace_id=str(link.bot_id),
            metadata={"message_id": first.message_id, "message_thread_id": first.message_thread_id, _SOURCES: [row.message_id for row in origins]},
        )

    @staticmethod
    def _button(kind, target, *, target_kind="attempt"):
        label = "label.cancel" if kind == "cancel_wait" else "label.try_again"
        return [[{"label": label, "kind": kind, "target_kind": target_kind, "target_id": target}]]

    async def _source(self, turn, key):
        from app.telegram_bots.publication import fixed_source

        source = await fixed_source(self.services, turn.link, key)
        if not self._current(turn):
            raise ValueError("chat-turn-retired") from None
        return source

    async def _input_outcome(self, turn, key, *, keyboard=()):
        if not self._current(turn):
            return None
        source = await self._source(turn, key)
        identity = await self.services.outbox.plan_input_outcome(
            turn.link,
            source_id=f"chat-input:{turn.link.chat_id}:{turn.link.binding_generation}:" + (turn.callback_id or str(turn.origins[0].message_id)),
            source_ref=source,
            keyboard=keyboard,
            callbacks=self.services.callbacks,
            guard=self._guard(turn, repair=True),
            inbox_rows=() if turn.callback_id else turn.origins,
            input_owner_callback_id=turn.callback_id,
        )
        turn.input_closed = identity is not None
        return identity

    async def _prepare(self, turn):
        from app.telegram_bots.threads import ThreadPreparationFailure

        # Grouping and staged-input reads grant no execution privilege. The
        # actual thread coordinator owns live target/config checks before HTTP.
        guard = self._guard(turn, creating=True, repair=True)
        if turn.stop_requested:
            await self._input_outcome(turn, "busy.cancelled")
            self._drop_waiting(turn)
            return False
        try:
            if turn.callback_id is None:
                if turn.submission is not None:
                    stage = await self.services.outbox.inbox.read_submission(turn.link, turn.submission, guard=guard)
                    guard.current()
                    turn.origins = stage.origins
                    if stage.record is not None:
                        turn.record = stage.record
                        return True
                    turn.origins = await self.services.outbox.inbox.group_prepublication(turn.origins, guard=guard)
                    thread = await self.services.threads.ensure_submission_thread(turn.link, stage)
                else:
                    turn.origins = await self.services.outbox.inbox.group_prepublication(turn.origins, guard=guard)
                    guard.current()
                    human_inputs = getattr(self.services, "human_inputs", None)
                    before = getattr(human_inputs, "before_plain_publication", None)
                    if before is not None:
                        await before(turn.channel, turn.link, turn.origins)
                        guard.current()
                    thread = await self.services.threads.ensure_thread(turn.link, turn.inbound, origin=turn.origins[0])
            else:
                stage = await self.services.outbox.inbox.read_callback_input(turn.link, turn.callback_id, guard=guard)
                guard.current()
                turn.origins = stage.origins
                if stage.record is not None:
                    turn.record = stage.record
                    return True
                thread = await self.services.threads.ensure_callback_thread(turn.link, callback_id=turn.callback_id)
        except ThreadPreparationFailure as failure:
            if not self._current(turn):
                return False
            key = "custom_agent.deleted" if failure.kind == "custom_agent_deleted" else "chat.files_full"
            await self._input_outcome(turn, key, keyboard=self._button("retry_message", turn.callback_id or str(turn.origins[0].message_id), target_kind="request"))
            return False
        if not self._current(turn):
            return False
        if turn.stop_requested:
            await self._input_outcome(turn, "busy.cancelled")
            self._drop_waiting(turn)
            return False
        if not self._creating(turn):
            return False
        endpoint = self.services.thread_client.endpoint_id
        attempt = await self.services.publication_authority.capture(turn.link, thread_id=thread, endpoint_id=endpoint)
        guard = self._guard(turn, creating=True, borrowed=attempt.create_guard)
        if turn.callback_id is None:
            record = await self.services.outbox.inbox.reserve_publication(turn.link, turn.origins, thread_id=thread, endpoint_id=endpoint, guard=guard)
        else:
            record = await self.services.outbox.inbox.reserve_callback_publication(turn.link, turn.callback_id, thread_id=thread, endpoint_id=endpoint, guard=guard)
        # The immutable publication is owned before any marker/enqueue. Stop can
        # now bind its barrier to this exact identity even while dispatch awaits.
        turn.record = record
        if not self._current(turn):
            return False
        if turn.stop_requested:
            await self._request_stop(turn)
            return True
        turn.record = await self.services.publications.dispatch_record(record, turn.link, saved_files=turn.saved_files)
        return True

    async def _view(self, turn):
        from app.telegram_bots.streaming import LivePresentation

        if not self._current(turn) or turn.record is None:
            return
        turn.presentation = LivePresentation(
            channel=turn.channel,
            link=turn.link,
            draft_id=secrets.randbelow((1 << 63) - 1) + 1,
            topic=turn.topic,
            is_current=lambda: self._current(turn),
            observer=self.observer,
            origins=() if turn.callback_id else turn.origins,
            reply_to_message_id=turn.origins[0].message_id,
        )
        try:
            await turn.presentation.start()
        except asyncio.CancelledError:
            raise
        except Exception:
            # Advisory UI failure cannot become a model failure or release the
            # queue; final source retains its own authority and retry path.
            await turn.presentation.close()

    async def _display_loop(self, turn, view):
        while self._current(turn) and turn.presentation is view and not view.closed:
            try:
                await view.flush()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                turn.held("chat_preview_held", error)
            await asyncio.sleep(1)

    async def _pause_ui(self, turn, *, final=False):
        if turn.presentation is None:
            return True
        return await turn.presentation.prepare_final() if final else await turn.presentation.pause()

    async def _waiting_notice(self, turn, failure):
        if turn.presentation is not None and not turn.presentation.closed:
            await turn.presentation.abandon()
        if turn.capacity_notice_sent:
            return
        if turn.waiting_since is None:
            self.queue.retry_later(turn.link, turn.inbound)
            turn.waiting_since = turn.channel._time()
            turn.retry_at = turn.waiting_since + self.services.config.queue.retry_seconds
        if not self._creating(turn):
            return
        await self.services.outbox.plan_product_keyboard(
            turn.link,
            source_id="capacity:" + turn.record.turn_id,
            parts=[{"kind": "text", "source_ref": {"key": failure.text_key, "values": {}}, "reply_to_message_id": turn.origins[0].message_id}],
            keyboard=self._button("cancel_wait", turn.record.turn_id),
            callbacks=self.services.callbacks,
            guard=self._guard(turn),
            message_thread_id=turn.topic,
        )
        turn.capacity_notice_sent = True

    async def _try_final(self, turn, operation):
        """One attempt at a finished run's reply. `_drive` repeats it, within a bound."""
        try:
            return await operation()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if turn.quiet_since is None:
                turn.held("chat_final_unplanned", error)
            if turn.presentation is not None:
                await turn.presentation.close()
            return None

    def _run_is_not_coming(self, turn, record):
        """A worker that has the message starts its run, however long a queue or a file takes.

        Once it lets go a run shows at once or never will. Stop fences a message still queued, so
        that one is not waited for, but only its worker can prove one it has taken will not start.
        """
        worker = self.services.publications.worker_state(record)
        if worker == "taken" or worker == "queued" and not turn.stop_requested:
            turn.quiet_since = None
            return False
        return self._overdue(turn, limit=RUN_SHOWS_SECONDS)

    def _overdue(self, turn, *, limit=None):
        """A turn holds its chat only while it shows life. The clock starts at its first quiet moment."""
        now = turn.channel._time()
        if turn.quiet_since is None:
            turn.quiet_since = now
        return now - turn.quiet_since >= (self.services.config.queue.max_wait_seconds if limit is None else limit)

    async def _publication_outcome(self, turn, key, *, run=None, refusal=None, undeliverable=False):
        from app.telegram_bots.publication import conversation_button

        source = await self._source(turn, key)
        on_web = key == "answer.on_web"
        return await self.services.outbox.plan_publication_outcome(
            turn.record,
            run=run,
            creation_refusal=refusal,
            source_ref=source,
            # The answer exists, so the way to it is the web. Running it again would end the same way.
            keyboard=() if on_web else self._button("retry_message", turn.record.publication_id),
            callbacks=self.services.callbacks,
            guard=self._guard(turn),
            undeliverable=undeliverable,
            url_button=await conversation_button(turn.link, turn.record) if on_web else None,
        )

    async def _settle_cancelled_inputs(self, turn):
        # Stop drops the ephemeral tail first; its durable inputs are closed by
        # their actual owner, never by fabricating a thread/publication.
        while turn.cancelled_waiting:
            entry = turn.cancelled_waiting[0]
            origins = self._callback_origins[(turn.link.id, entry.callback_id)][1] if entry.callback_id else tuple(self._origins[(turn.link.id, identity)] for identity in entry.inbound.metadata[_SOURCES])
            held = _Turn(turn.channel, turn.link, entry.inbound, origins, callback_id=entry.callback_id)
            guard = _Guard(self.services, turn.channel, turn.link, lambda: self._current(turn), repair=True)
            if turn.waiting_told and entry.callback_id is None:
                # The stop's own text says these were not answered, so one line does for all of them.
                await guard.prepare()
                guard.current()
                for origin in origins:
                    await self.services.outbox.inbox.finish_route(origin)
            else:
                await self.services.outbox.plan_input_outcome(
                    turn.link,
                    source_id="chat-cancelled:" + (entry.callback_id or str(origins[0].message_id)),
                    source_ref={"key": "busy.cancelled", "values": {}},
                    keyboard=(),
                    callbacks=self.services.callbacks,
                    guard=guard,
                    inbox_rows=() if held.callback_id else origins,
                    input_owner_callback_id=held.callback_id,
                )
            if not self._current(turn):
                return
            if entry.callback_id is not None:
                self._callback_origins.pop((turn.link.id, entry.callback_id), None)
            else:
                self._let_go(turn.link, [origin.message_id for origin in origins])
            turn.cancelled_waiting = turn.cancelled_waiting[1:]
        for row in turn.stop_commands:
            guard = self._guard(turn, repair=True)
            await guard.prepare()
            guard.current()
            await self.services.outbox.inbox.finish_route(row)
            guard.current()
        turn.stop_commands.clear()

    @staticmethod
    def _report_wait(turn, run, worker=None):
        """A run that never ends, or a queue that never drains, writes nothing else, and it holds the whole chat."""
        now = turn.channel._time()
        if turn.observed_since is None:
            turn.observed_since = now
        if not turn.wait_reported and now - turn.observed_since >= TURN_WAIT_REPORT_SECONDS:
            turn.wait_reported = True
            turn.held("chat_turn_waiting", status=run.row.get("status") if run is not None else None, worker=worker, waited_s=int(now - turn.observed_since))

    async def _complete(self, turn):
        if not self._current(turn):
            return
        await self._settle_cancelled_inputs(turn)
        if turn.presentation is not None:
            await turn.presentation.close()
        self._finish(turn)

    async def _observe(self, turn, operation):
        """Retry guarded reads or exact idempotent plans, never creating work."""
        reported = False
        while self._current(turn):
            try:
                return await operation()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                if not reported:
                    turn.held("chat_observation_held", error)
                    reported = True
                if self._overdue(turn):
                    raise _TurnStalled() from error
                if turn.presentation is not None:
                    await turn.presentation.close()
                turn.wake.clear()
                try:
                    await asyncio.wait_for(turn.wake.wait(), 1)
                except TimeoutError:
                    pass
        return None

    async def _drive(self, turn):
        from app.telegram_bots.errors import classify_creation_refusal, classify_run
        from app.telegram_bots.publication import channel_execution_scope

        if not self._creating(turn):
            return
        try:
            prepared = await self._prepare(turn)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if turn.record is None and turn.stop_requested and self._current(turn):
                # Preparation has settled after Stop withdrew its creating
                # guard. This writer proves there is still no publication;
                # an ambiguous reserve result cannot masquerade as no dispatch.
                await self._observe(turn, lambda: self._input_outcome(turn, "busy.cancelled"))
                self._drop_waiting(turn)
                await self._observe(turn, lambda: self._complete(turn))
                return
            if turn.record is None or not self._current(turn):
                raise
            # A lost creating result cannot authorize a second dispatch. The
            # pinned immutable publication still owns read-only discovery/Stop.
            turn.held("chat_dispatch_result_held", error)
            prepared = True
        if not prepared:
            # A positive terminal input plan, unlike an absent run, owns this
            # completion. Retirement/phase closure leaves the input for startup.
            if self._current(turn) and turn.input_closed:
                await self._observe(turn, lambda: self._complete(turn))
            return
        watcher = display = None
        try:
            while self._current(turn):
                record = turn.record
                if watcher is None:
                    await self._view(turn)
                    if turn.presentation is not None:
                        display = self._spawn(self._display_loop(turn, turn.presentation), channel=turn.channel, turn=turn)
                if watcher is None or watcher.done():
                    watcher = self._spawn(
                        self.observer.watch(record, turn.link, on_preview=lambda value: self._preview(turn, value), on_line=lambda key, values: self._activity(turn, key, values), turn_done=turn.done),
                        channel=turn.channel,
                        turn=turn,
                    )
                if turn.stop_requested:
                    await self._observe(turn, lambda: self._request_stop(turn))
                    if turn.done.is_set():
                        return
                run = await self._observe(turn, lambda: self.observer.read_run(record, turn.link))
                if not self._current(turn):
                    return
                if run is not None:
                    # A run first seen, or one still working, is life. A run that has ended is not.
                    if run.row.get("run_id") != turn.run_id or run.row.get("status") not in _RUN_ENDED:
                        turn.quiet_since = None
                    turn.run_id = run.row.get("run_id")
                failure = classify_creation_refusal(turn.refusal, channel_execution_scope(record)) if turn.refusal is not None else None
                if run is not None and run.row.get("status") in _RUN_ENDED:
                    failure = classify_run(run, channel_execution_scope(record))
                    stopped = turn.stop_requested and run.row.get("status") == "interrupted"
                    overdue = False
                    if stopped or failure is None:
                        if await self._observe(turn, lambda: self._pause_ui(turn, final=True)):
                            inputs = getattr(self.services, "human_inputs", None)
                            if not stopped and inputs is not None:
                                planned = await self._try_final(turn, lambda: inputs.plan_terminal(record, turn.link))
                                if planned is not None:
                                    await self._observe(turn, lambda: self._complete(turn))
                                    return
                            waiting = bool(turn.cancelled_waiting) if turn.waiting_dropped else bool(self.queue.waiting_count(turn.link, turn.topic))
                            identity = await self._try_final(turn, lambda: self.observer.plan_final(record, turn.link, stopped=stopped, waiting=waiting))
                            if identity is not None:
                                if stopped:
                                    self._drop_waiting(turn)
                                    turn.waiting_told = waiting
                                await self._observe(turn, lambda: self._complete(turn))
                                return
                            # A held turn stops this whole chat, so the wait for a
                            # finished run's reply has a bound.
                            overdue = self._overdue(turn)
                            if overdue and failure is None:
                                # This plan can lose to a reply that did commit, and
                                # the next `plan_final` then returns that reply.
                                # Only an answer that exists can be pointed at. A run that wrote none can still be tried again.
                                key = "answer.on_web" if self.observer.final_read(record).get("output", "").startswith("complete:") else "error.generic"
                                if await self._try_final(turn, lambda: self._publication_outcome(turn, key, run=run, undeliverable=True)) is not None:
                                    turn.held("chat_final_undeliverable", **self.observer.final_read(record))
                                    await self._observe(turn, lambda: self._complete(turn))
                                    return
                                if turn.error_plan_refused:
                                    raise _TurnStalled()
                                turn.error_plan_refused = True
                        elif self._overdue(turn):
                            raise _TurnStalled()
                        # A stopped stream still requires acknowledged exact
                        # cancellation and unavailable(cancelled) source proof.
                        # Past the bound it ends as the interrupted run it is.
                        if stopped and not overdue:
                            failure = None
                if failure is not None:
                    if failure.retry_later:
                        turn.capacity_waiting = True
                        if turn.stop_status == "terminal":
                            # The prior run is terminal, but fresh typed proof
                            # now establishes a logical capacity wait (4.1).
                            # The kept same-turn gesture takes that wait out of
                            # line; it never claims that a run was cancelled.
                            turn.cancel_future = True
                        if turn.cancel_future:
                            if turn.presentation is not None:
                                await turn.presentation.abandon()
                            self._drop_waiting(turn)
                            await self._observe(turn, lambda: self._publication_outcome(turn, "busy.cancelled", run=run, refusal=turn.refusal if run is None else None))
                            await self._observe(turn, lambda: self._complete(turn))
                            return
                        await self._observe(turn, lambda: self._waiting_notice(turn, failure))
                        if not self._current(turn):
                            return
                        now = turn.channel._time()
                        if now - turn.waiting_since >= self.services.config.queue.max_wait_seconds:
                            await self._observe(turn, lambda: self._publication_outcome(turn, "busy.gave_up", run=run, refusal=turn.refusal if run is None else None))
                            await self._observe(turn, lambda: self._complete(turn))
                            return
                        if self._creating(turn) and now >= turn.retry_at:
                            old = record

                            def pin(successor):
                                if not self._current(turn):
                                    raise ValueError("chat-turn-retired") from None
                                turn.record = successor
                                turn.run_id = None
                                turn.refusal = None
                                turn.stop_status = None
                                turn.stop_marker_checked = False
                                turn.capacity_waiting = False
                                if turn.cancel_future:
                                    raise ValueError("chat-turn-stop-before-retry-marker") from None

                            try:
                                if turn.refusal is None:
                                    turn.record = await self.services.publications.retry(old, turn.link, on_reserved=pin, saved_files=turn.saved_files)
                                else:
                                    turn.record = await self.services.publications.retry_refused(old, turn.link, turn.refusal, on_reserved=pin, saved_files=turn.saved_files)
                            except asyncio.CancelledError:
                                raise
                            except Exception as error:
                                if not self._current(turn):
                                    raise
                                turn.held("chat_retry_result_held", error)
                                if turn.record.publication_id == old.publication_id:
                                    # Nothing replaced it, so the same wait goes on: `retry_at`
                                    # paces it and the deadline above ends it.
                                    turn.retry_at = now + self.services.config.queue.retry_seconds
                                    continue
                                # The successor was positively handed off before
                                # its creating result became uncertain. Observe
                                # only that identity; never redispatch it.
                            self.observer.retire(old)
                            if watcher is not None and not watcher.done():
                                watcher.cancel()
                            await asyncio.gather(watcher, return_exceptions=True)
                            watcher = None
                            if display is not None:
                                display.cancel()
                                await asyncio.gather(display, return_exceptions=True)
                                display = None
                            turn.retry_at = now + self.services.config.queue.retry_seconds
                            continue
                    else:
                        if await self._observe(turn, lambda: self._pause_ui(turn, final=True)):
                            turn.held("chat_run_failed", status=run.row.get("status") if run is not None else None, outcome=failure.text_key)
                            await self._observe(turn, lambda: self._publication_outcome(turn, failure.text_key, run=run, refusal=turn.refusal if run is None else None))
                            await self._observe(turn, lambda: self._complete(turn))
                            return
                        elif self._overdue(turn):
                            raise _TurnStalled()
                self._report_wait(turn, run, self.services.publications.worker_state(record) if run is None else None)
                if run is None and failure is None and self._run_is_not_coming(turn, record):
                    # The watcher polls for the run, so look once more before calling it gone.
                    located = await self._observe(turn, lambda: self.services.publications.locate_run(record, turn.link, max_pages=8))
                    if located is None or located.state != "matched":
                        raise _TurnStalled()
                    turn.quiet_since = None
                turn.wake.clear()
                try:
                    await asyncio.wait_for(turn.wake.wait(), 1)
                except TimeoutError:
                    pass
        finally:
            owned = [task for task in (watcher, display) if task is not None]
            for task in owned:
                if not task.done():
                    task.cancel()
            await await_drained(asyncio.gather(*owned, return_exceptions=True))
            if turn.presentation is not None:
                # A failed read keeps durable input held but retires its
                # ephemeral UI, including permission for pending Thinking rows.
                await turn.presentation.close()

    async def _request_stop(self, turn):
        async with turn.stop_lock:
            if not self._current(turn) or turn.record is None or turn.stop_id is None:
                return
            if turn.stop_status in {None, "unknown"}:
                await self._pause_ui(turn)
                if not self._current(turn):
                    return
                turn.stop_status = await self.observer.request_stop(turn.record, turn.link, stop_id=turn.stop_id)
                if not self._current(turn):
                    return
            elif turn.stop_status == "pending":
                decision = self.observer.stop_outcome(turn.record, turn.link, stop_id=turn.stop_id)
                if decision is not None:
                    turn.stop_status = decision
            if turn.stop_status == "requested":
                self._drop_waiting(turn)
            if turn.stop_status in {"refused", "terminal"}:
                turn.stop_requested = False
                turn.cancel_future = False
                turn.stop_id = None
                if turn.presentation is not None:
                    await turn.presentation.resume()
                press, turn.stop_press = turn.stop_press, None
                if turn.stop_status == "refused":
                    for row in turn.stop_commands:
                        await turn.channel.send_text("stop.failed", source_id=f"chat-stop-refused:{row.chat_id}:{row.message_id}", inbox_rows=[row], finish_route=True, thread=turn.topic, _captured_link=turn.link)
                    turn.stop_commands.clear()
                    if press is not None:
                        row, update_id = press
                        await turn.channel.send_text("stop.failed", source_id=f"press:{row.id}:{update_id}", reply_to=row.message_id, thread=turn.topic, _captured_link=turn.link)
                return
            if turn.stop_status == "pending" and not turn.stop_marker_checked:
                record = turn.record
                attempt = await self.services.publication_authority.capture(turn.link, thread_id=record.scope.thread_id, endpoint_id=record.scope.endpoint_id)
                guard = self._guard(turn, borrowed=attempt.cancel_guard)
                fresh, _ = await self.services.outbox.inbox.read_publication_input(record, guard=guard)
                if not self._current(turn):
                    return
                turn.record = fresh
                if fresh.dispatch_started is False and fresh.run_id is None:
                    self._drop_waiting(turn)
                    await self.services.outbox.plan_cancelled_before_dispatch(fresh, stop_id=turn.stop_id, guard=guard, callbacks=self.services.callbacks)
                    await self._complete(turn)
                turn.stop_marker_checked = True

    def _drop_waiting(self, turn):
        if not self._current(turn):
            return
        if not turn.waiting_dropped:
            turn.cancelled_waiting = self.queue.waiting_inputs(turn.link, turn.topic)
            self.queue.cancel_wait(turn.link, turn.topic)
            turn.waiting_dropped = True

    async def _stop_turn(self, turn, *, stop_id):
        if not self._current(turn):
            return
        turn.stop_requested = True
        turn.cancel_future = True
        turn.quiet_since = None
        if turn.record is not None and turn.run_id is None:
            # A message a worker has not started must not start now. A POST already sent shows as a run.
            self.services.publications.fence(turn.record)
        if turn.stop_id is None:
            turn.stop_id = stop_id
            turn.stop_status = None
            turn.stop_marker_checked = False
        if turn.record is None and turn.task is not None and turn.task.done():
            # The original preparation has already drained. The public input
            # writer rejects any publication that raced a lost reserve result.
            await self._input_outcome(turn, "busy.cancelled")
            self._drop_waiting(turn)
            await self._complete(turn)
            return
        # Block an undispatched marker before awaiting any disposable UI. This
        # writes only the Stop barrier; it neither freezes shown ranges nor
        # sends cancellation before the display acknowledgement has settled.
        for _ in range(2):
            record = turn.record
            if record is None or not self._current(turn):
                break
            attempt = await self.services.publication_authority.capture(turn.link, thread_id=record.scope.thread_id, endpoint_id=record.scope.endpoint_id)
            fresh = await self.services.outbox.inbox.request_publication_stop(record, stop_id=turn.stop_id, guard=self._guard(turn, borrowed=attempt.cancel_guard))
            if not self._current(turn):
                return
            if turn.record.publication_id == record.publication_id:
                turn.record = fresh
                break
        # An actual in-flight preview acknowledgement settles before source
        # ranges are frozen by the observer's cancellation request.
        await self._pause_ui(turn)
        if self._current(turn) and turn.record is not None:
            await self._request_stop(turn)
        turn.wake.set()

    async def stop_chat(self, channel, update):
        async with self._entry(channel):
            link = deepcopy(channel._admission(update).link)
            topic = None if update.message.message_thread_id in (None, 1) else update.message.message_thread_id
            await self.flush_inputs(channel, topic=topic)
            original = self._turns.get((link.id, topic))
            if self._closed or channel in self._retired or not await channel._input_current(link) or not channel._callback_current(link):
                return
            row, created = await await_drained(self.services.outbox.inbox.accept(link, update, sanitized_input={"command": "stop"}))
            if not created and not self.unfinished(link, row):
                channel._classified(update, remember=True)
                return
            if not self._open(channel, link):
                return
            human_inputs = getattr(self.services, "human_inputs", None)
            if human_inputs is not None:
                await human_inputs.command(channel, update, "stop", captured_link=link)
            if not self._open(channel, link):
                return
            row = await await_drained(self.services.outbox.inbox.route(row, kind="command", target_id="stop", basis={}, operation_ids=[]))
            if not channel._callback_current(link):
                return
            turn = original
            if turn is None or turn.channel is not channel or not self._current(turn):
                await channel.send_text("stop.nothing", source_id=f"chat-stop:{row.chat_id}:{row.message_id}", inbox_rows=[row], finish_route=True, thread=row.message_thread_id, _captured_link=link)
            else:
                turn.stop_commands.append(row)
                await self._stop_turn(turn, stop_id=f"command:{row.chat_id}:{row.message_id}")
            channel._classified(update, remember=True)

    async def native_stop(self, channel, update):
        async with self._entry(channel):
            data = update.api_kwargs.get("stopped_message_generation")
            if type(data) is not dict or type(data.get("chat")) is not dict or type(data["chat"].get("id")) is not int or type(data.get("draft_id")) is not int or data["draft_id"] <= 0:
                return
            for turn in tuple(self._turns.values()):
                if turn.channel is channel and self._current(turn) and turn.presentation is not None and turn.presentation.matches_stop(chat_id=data["chat"]["id"], draft_id=data["draft_id"]):
                    await self._stop_turn(turn, stop_id=f"native:{update.update_id}:{data['draft_id']}")
                    return

    def owns_publication(self, record, link):
        """Defer recovery presentation while this original live owner can freeze UI."""
        for turn in self._turns.values():
            # The identity only: a poll rewrites the link's stamps, and a whole-row compare would lapse after a minute.
            # A closing turn has let go, so what it plans is not held back for it.
            if self._current(turn) and not turn.closing and turn.record is not None and turn.record.scope == record.scope and turn.record.publication_id == record.publication_id and turn.link.id == link.id:
                return True
        return False

    async def _spend_buttons(self, channel, link, row):
        from app.telegram_bots.producer import keyboard_cleanup_row

        guard = _Guard(self.services, channel, link, lambda: self._open(channel, link), repair=True)
        async with self.services.callbacks.writer(link, guard) as writer:
            group = await self.services.callbacks.lock_current(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard, session=writer)
            await self.services.callbacks.consume_group(group, session=writer)
            writer.add(keyboard_cleanup_row(group.selected, owner_user_id=link.owner_user_id, now=self.services.outbox.clock()))

    async def execute_callback(self, channel, row, *, update_id, query_id):
        from app.telegram_bots.callbacks import CallbackRefused

        async with self._entry(channel):
            link = deepcopy(channel.link)
            if self._closed or channel in self._retired or (row.link_id, row.chat_id, row.binding_generation) != (link.id, link.chat_id, link.binding_generation) or not channel._callback_current(link):
                raise CallbackRefused() from None
            turn = self._turns.get((link.id, row.message_thread_id))
            if row.kind in {"stop_answer", "cancel_wait"}:
                if turn is None or turn.channel is not channel or not self._current(turn):
                    raise CallbackRefused() from None
                if turn.stop_requested:
                    return  # A repeat tap: the first one is doing this.
                if row.kind == "cancel_wait":
                    if turn.record is None or row.target_kind != "attempt" or row.target_id != turn.record.turn_id:
                        raise CallbackRefused() from None
                    if turn.capacity_waiting is not True:
                        raise CallbackRefused("stop.failed") from None  # It is being tried right now, and may yet wait again.
                else:
                    view = turn.presentation
                    if view is None or not await view.matches_callback(row):
                        raise CallbackRefused() from None
                    if not self._current(turn) or turn.presentation is not view or row.target_kind != "draft" or row.target_id != str(view.draft_id):
                        raise CallbackRefused() from None
                turn.stop_press = (row, update_id)
                await self._stop_turn(turn, stop_id="callback:" + row.id)
                if row.kind == "cancel_wait" and turn.stop_status != "refused":
                    await await_drained(self._spend_buttons(channel, link, row))
                return
            if row.kind not in {"retry_message", "file_retry"}:
                raise CallbackRefused() from None

            guard = _Guard(self.services, channel, link, lambda: self._open(channel, link), repair=True)
            from app.telegram_bots.callbacks import _RESERVED

            business = {key: value for key, value in row.payload.items() if key not in _RESERVED}
            try:
                if row.kind == "file_retry" and set(business) == {"thread_id", "path"}:
                    source = getattr(self.services, "model_source", None)
                    if source is None:
                        raise CallbackRefused() from None
                    await source.retry_file_callback(link, row, callbacks=self.services.callbacks, guard=guard)
                    return
                stage = await self.services.outbox.inbox.stage_callback_input(
                    link,
                    callback_id=row.id,
                    update_id=update_id,
                    chat_id=row.chat_id,
                    message_id=row.message_id,
                    message_thread_id=row.message_thread_id,
                    callbacks=self.services.callbacks,
                    guard=guard,
                )
            except ValueError:
                # What this would try again is gone, and tapping again cannot bring it back.
                raise CallbackRefused() from None
            guard.current()
            if stage.route_complete:
                return
            # Replay of an already-owned current/waiting unit never acquires a
            # second queue position or joins its immutable input with new text.
            if any(active.callback_id == row.id for active in self._turns.values()) or any(entry.callback_id == row.id for entry in self.queue.waiting_inputs(link, row.message_thread_id)):
                return
            inbound = self._inbound(link, stage.origins)
            result = self.queue.admit_callback(link, inbound, callback_id=row.id)
            if result == "full":
                await self.services.outbox.plan_input_outcome(
                    link,
                    source_id="callback-full:" + row.id,
                    source_ref={"key": "busy.followup_full", "values": {}},
                    keyboard=self._button("retry_message", row.id, target_kind="request"),
                    callbacks=self.services.callbacks,
                    guard=guard,
                    input_owner_callback_id=row.id,
                )
                return
            self._callback_origins[(link.id, row.id)] = (channel, stage.origins)
            if result == "run":
                self._start(channel, link, inbound)

    async def recover_startup(self):
        """Every input the last process left open, while the host still holds ingress.

        This is a fixed restart disposition, never model retry or a fabricated
        publication. Unknown/unrouted/incomplete groups stay retained.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + LEFT_INPUTS_SECONDS
        host = self.services

        def phase():
            return (
                not self._closed
                and asyncio.get_running_loop() is loop
                and loop.time() < deadline
                and host._copy_decision is False
                and host._cohort_complete is True
                and host._ready is True
                and host._send_ready is False
                and not host._stopping
                and not host._stopped
                and host._cause is None
            )

        await self._close_left_inputs(phase)

    async def recover_link(self, channel):
        """A starting channel owns nothing yet, so what its link saved was left by the one before it.

        The caller holds this channel's polling back until this returns.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + LEFT_INPUTS_SECONDS
        link_id = channel.link.id
        await self._close_left_inputs(lambda: not self._closed and loop.time() < deadline and self.services.channel(link_id) is channel, link_id=link_id)

    async def _close_left_inputs(self, phase, *, link_id=None):
        after = None
        while phase():
            # Retained SQL may settle after D. Its result never re-arms the phase.
            try:
                page = await await_drained(self.services.outbox.inbox.prepublication_page(after=after, limit=32, max_origins=64, link_id=link_id))
            except asyncio.CancelledError:
                raise
            except Exception as error:
                _chat_line("chat_startup_page_held", error=error)
                return
            for entry in page.entries:
                if not phase():
                    return
                if entry.hold_reason not in (None, "voice_interrupted") or not entry.origins:
                    continue
                try:
                    await self._recover_input(entry, phase)
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    # A stale or unreadable member cannot admit itself, and
                    # cannot suppress another independent input's outcome.
                    _chat_line("chat_startup_input_held", error=error, link=entry.key.link_id)
            if not page.has_more:
                return
            after = page.next_after

    async def _recover_input(self, entry, phase):
        host = self.services
        link = await await_drained(host.links.get(entry.key.link_id))
        if not phase() or link is None:
            return
        channel = host.channel(link.id)
        if channel is None:
            return
        captured = deepcopy(link)

        def current():
            # A channel whose first start failed is still this link's, and this only writes a plan.
            return phase() and host.channel(link.id) is channel and channel._same_scope(captured, channel.link) and channel.transport_revision == captured.token_revision and not channel._stopping

        guard = _Guard(host, channel, captured, current, repair=True)
        if not current():
            return
        # A note interrupted while working may have reached its provider, so it
        # is never transcribed again. Nothing else in the process owns it now.
        interrupted = entry.hold_reason == "voice_interrupted"
        await host.outbox.plan_input_outcome(
            captured,
            source_id=f"chat-restart:{captured.chat_id}:{captured.binding_generation}:" + (entry.input_owner_callback_id or str(entry.origins[0].message_id)),
            source_ref={"key": "voice.failed" if interrupted else "chat.restarted", "values": {}},
            keyboard=() if interrupted else self._button("retry_message", entry.input_owner_callback_id or str(entry.origins[0].message_id), target_kind="request"),
            callbacks=host.callbacks,
            guard=guard,
            inbox_rows=() if entry.input_owner_callback_id else entry.origins,
            input_owner_callback_id=entry.input_owner_callback_id,
        )

    async def _preview(self, turn, value):
        if self._current(turn) and getattr(turn, "presentation", None) is not None:
            await turn.presentation.preview(value)

    async def _activity(self, turn, key, values):
        if self._current(turn) and getattr(turn, "presentation", None) is not None:
            await turn.presentation.activity(key, values)

    async def outbound(self, channel, message):
        from app.channels.outcomes import CreationRefusedMessage
        from app.telegram_bots.errors import classify_creation_refusal
        from app.telegram_bots.publication import channel_execution_scope

        if self._closed or channel in self._retired:
            return True
        for turn in tuple(self._turns.values()):
            if turn.channel is not channel or not self._current(turn) or turn.record is None:
                continue
            scope = turn.record.scope
            if (message.chat_id, message.thread_id, message.owner_user_id, message.connection_id) != (str(scope.chat_id), scope.thread_id, scope.owner_user_id, scope.connection_id):
                continue
            if type(message) is CreationRefusedMessage:
                try:
                    classify_creation_refusal(message.creation_refusal, channel_execution_scope(turn.record))
                except Exception:
                    continue
                turn.refusal = message.creation_refusal
            # Plain bus text is only a wake-up; it never proves a final or a
            # creating refusal, even when it quotes a manager sentence.
            turn.wake.set()
        return True

    def message_sent(self, channel, *, chat_id, topic):
        for turn in tuple(self._turns.values()):
            if turn.channel is channel and self._current(turn) and turn.presentation is not None:
                turn.presentation.message_sent(chat_id=chat_id, topic=topic)

    def has_turn(self, channel, *, topic):
        turn = self._turns.get((channel.link.id, None if topic in (None, 1) else topic))
        return turn is not None and turn.channel is channel and self._current(turn)

    def typing_allowed(self, channel, *, topic):
        topic = None if topic in (None, 1) else topic
        turn = self._turns.get((channel.link.id, topic))
        view = None if turn is None else turn.presentation
        return not (turn is not None and turn.channel is channel and self._current(turn) and view is not None and not view.closed and view.mode == "draft" and view.started and not view.paused)

    def preview_allowed(self, channel, item):
        return any(turn.channel is channel and self._current(turn) and turn.presentation is not None and turn.presentation.allows_placeholder(item) is True for turn in tuple(self._turns.values()))

    def _retire_now(self, channel):
        self._retired.add(channel)
        for key, joined in tuple(self._joins.items()):
            if joined.channel is channel:
                self._joins.pop(key)
                if joined.task is not None:
                    joined.task.cancel()
        retired = []
        for turn in tuple(self._turns.values()):
            if turn.channel is not channel:
                continue
            self._forget(turn)
            retired.append(turn)
        for key, owner in tuple(self._origin_channels.items()):
            if owner is channel:
                self._origin_channels.pop(key, None)
                self._origins.pop(key, None)
        for key, (owner, _origins) in tuple(self._callback_origins.items()):
            if owner is channel:
                self._callback_origins.pop(key, None)
        return retired

    async def retire(self, channel):
        task = self._retire_tasks.get(channel)
        if task is None:
            retired = self._retire_now(channel)
            own = asyncio.current_task()

            async def settle():
                pending = {task for task, owner in [*self._task_channels.items(), *self._entries.items()] if owner is channel and task is not own and not task.done()}
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                results = await asyncio.gather(*(turn.presentation.close() for turn in retired if turn.presentation is not None), return_exceptions=True)
                for result in results:
                    if isinstance(result, BaseException):
                        raise result

            task = asyncio.create_task(settle(), name="telegram-chat-retirement")
            self._retire_tasks[channel] = task
        try:
            await await_drained(task)
        finally:
            if task.done() and self._retire_tasks.get(channel) is task:
                self._retire_tasks.pop(channel, None)

    async def stop(self):
        if self._stop_task is None or self._stop_task.done() and (self._stop_task.cancelled() or self._stop_task.exception() is not None):
            self._closed = True
            channels = {turn.channel for turn in self._turns.values()} | set(self._entries.values()) | {channel for channel in self._task_channels.values() if channel is not None}
            self.queue.close()

            async def settle():
                results = await asyncio.gather(*(self.retire(channel) for channel in channels), return_exceptions=True)
                if self._tasks:
                    for task in tuple(self._tasks):
                        task.cancel()
                    await asyncio.gather(*tuple(self._tasks), return_exceptions=True)
                if self._observer is not None:
                    await self.observer.drain()
                self._turns.clear()
                self._origins.clear()
                self._origin_channels.clear()
                self._callback_origins.clear()
                for result in results:
                    if isinstance(result, BaseException):
                        raise result

            self._stop_task = asyncio.create_task(settle(), name="telegram-chat-stop")
        await await_drained(self._stop_task)
