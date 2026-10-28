"""Owned chat turns; durable input precedes ephemeral queue and execution work."""

from __future__ import annotations

import asyncio
import logging
import secrets
import weakref
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import dataclass, field

from app.channels.message_bus import InboundMessage
from app.telegram_bots.queue import ChatQueue
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)
_SOURCES = "telegram_source_message_ids"


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

    @property
    def topic(self):
        return self.origins[0].message_thread_id


class ChatTurns:
    def __init__(self, services):
        self.services = services
        self.queue = ChatQueue(services.config)
        self._turns, self._origins = {}, {}
        self._tasks = set()
        self._task_channels = {}
        self._entries = {}
        self._retired = weakref.WeakSet()
        self._retire_tasks = {}
        self._origin_channels = {}
        self._closed = False
        self._observer = None
        self._stop_task = None

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

    def _spawn(self, coroutine, *, channel=None):
        task = asyncio.create_task(coroutine, name="telegram-chat-turn")
        self._tasks.add(task)
        self._task_channels[task] = channel

        def finished(done):
            self._tasks.discard(done)
            self._task_channels.pop(done, None)
            if not done.cancelled() and done.exception() is not None:
                logger.warning("telegram_bots chat_turn_held error=%s", type(done.exception()).__name__)

        task.add_done_callback(finished)
        return task

    async def accept(self, channel, update):
        async with self._entry(channel):
            return await self._accept(channel, update)

    async def _accept(self, channel, update):
        link, message = deepcopy(channel._admission(update).link), update.message
        if self._closed or channel in self._retired or message is None or type(message.text) is not str or not message.text.strip():
            return False
        if not await channel._input_current(link):
            return False
        if self._closed or channel in self._retired or not channel._callback_current(link):
            return False
        row, created = await await_drained(self.services.outbox.inbox.accept(link, update, sanitized_input={"type": "text", "text": message.text}))
        if self._closed or channel in self._retired or not channel._callback_current(link):
            channel._classified(update, remember=True)
            return True
        if not created:
            # Recovery owns retained identities, including input saved before a
            # dispatch. A repeated provider update never creates a new attempt.
            channel._classified(update, remember=True)
            return True
        forwarded = message.forward_origin is not None or message.is_automatic_forward is True or any(message.api_kwargs.get(key) is not None for key in ("forward_from", "forward_from_chat", "forward_sender_name", "forward_date"))
        row = await await_drained(self.services.outbox.inbox.route(row, kind="turn", target_id=link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": forwarded}}, operation_ids=[]))
        if self._closed or channel in self._retired or not channel._callback_current(link):
            channel._classified(update, remember=True)
            return True
        if forwarded:
            await channel.send_text("media.unsupported", source_id=f"chat-forwarded:{row.chat_id}:{row.message_id}", reply_to=row.message_id, thread=row.message_thread_id, inbox_rows=[row], finish_route=True, _captured_link=link)
            channel._classified(update, remember=True)
            return True
        inbound = InboundMessage(
            channel_name="telegram",
            chat_id=str(link.chat_id),
            user_id=str(link.telegram_user_id),
            text=message.text,
            thread_ts=str(message.message_id),
            topic_id=str(message.message_thread_id) if message.message_thread_id not in (None, 1) else None,
            connection_id=link.connection_id,
            owner_user_id=link.owner_user_id,
            workspace_id=str(link.bot_id),
            metadata={"message_id": message.message_id, "message_thread_id": row.message_thread_id, _SOURCES: [message.message_id]},
        )
        disposition = self.queue.admit(link, inbound)
        if disposition == "full":
            await channel.send_text("busy.followup_full", source_id=f"chat-full:{row.chat_id}:{row.message_id}", reply_to=row.message_id, thread=row.message_thread_id, inbox_rows=[row], finish_route=True, _captured_link=link)
        else:
            self._origins[(link.id, row.message_id)] = row
            self._origin_channels[(link.id, row.message_id)] = channel
            if disposition == "run":
                self._start(channel, link, inbound)
            elif disposition == "first-waiting":
                await channel.send_text("busy.followup", source_id=f"chat-wait:{row.chat_id}:{row.message_id}", reply_to=row.message_id, thread=row.message_thread_id, inbox_rows=[row], _captured_link=link)
            else:
                self._spawn(channel.react_waiting(link, row), channel=channel)
        channel._classified(update, remember=True)
        return True

    def _start(self, channel, link, inbound):
        origins = tuple(self._origins[(link.id, identity)] for identity in inbound.metadata[_SOURCES])
        turn = _Turn(channel, link, inbound, origins)
        turn.callback_id = self.queue.current_callback(link, turn.topic)
        self._turns[(link.id, turn.topic)] = turn
        if self._creating(turn):
            turn.task = self._spawn(self._drive(turn), channel=channel)

    def _finish(self, turn):
        if not self._current(turn) or turn.done.is_set():
            return
        turn.done.set()
        if turn.record is not None and self._observer is not None:
            self.observer.retire(turn.record)
        for row in turn.origins:
            self._origins.pop((turn.link.id, row.message_id), None)
            self._origin_channels.pop((turn.link.id, row.message_id), None)
        next_input = self.queue.turn_ended(turn.link, turn.topic)
        self._turns.pop((turn.link.id, turn.topic), None)
        if next_input is not None:
            self._start(turn.channel, turn.link, next_input)

    def _guard(self, turn, *, creating=False, repair=False, borrowed=None):
        current = (lambda: self._creating(turn) and not turn.stop_requested) if creating else lambda: self._current(turn)
        return _Guard(self.services, turn.channel, turn.link, current, repair=repair, borrowed=borrowed)

    @staticmethod
    def _inbound(link, origins):
        first = origins[0]
        return InboundMessage(
            channel_name="telegram",
            chat_id=str(link.chat_id),
            user_id=str(link.telegram_user_id),
            text="\n\n".join(row.sanitized_input["text"] for row in origins),
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
        values = {}
        if key == "custom_agent.deleted":
            values = {"product": self.services.product_name, "agent": turn.link.bot_username or self.services.product_name}
        elif key == "chat.files_full":
            values = {"product": self.services.product_name, "support": "an administrator"}
        elif key == "refused.interrupted":
            original = await self.services.prepare_ingress_values(turn.link)
            if not self._current(turn):
                raise ValueError("chat-turn-retired") from None
            values = {"name": original["name"]}
        return {"key": key, "values": values}

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
                turn.origins = await self.services.outbox.inbox.group_prepublication(turn.origins, guard=guard)
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
        turn.record = await self.services.publications.dispatch_record(record, turn.link)
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
            except Exception:
                logger.warning("telegram_bots chat_preview_held")
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

    async def _publication_outcome(self, turn, key, *, run=None, refusal=None):
        source = await self._source(turn, key)
        return await self.services.outbox.plan_publication_outcome(
            turn.record,
            run=run,
            creation_refusal=refusal,
            source_ref=source,
            keyboard=self._button("retry_message", turn.record.publication_id),
            callbacks=self.services.callbacks,
            guard=self._guard(turn),
        )

    async def _settle_cancelled_inputs(self, turn):
        # Stop drops the ephemeral tail first; its durable inputs are closed by
        # their actual owner, never by fabricating a thread/publication.
        while turn.cancelled_waiting:
            entry = turn.cancelled_waiting[0]
            origins = tuple(self._origins[(turn.link.id, identity)] for identity in entry.inbound.metadata[_SOURCES])
            held = _Turn(turn.channel, turn.link, entry.inbound, origins, callback_id=entry.callback_id)
            guard = _Guard(self.services, turn.channel, turn.link, lambda: self._current(turn), repair=True)
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
            for origin in origins:
                self._origins.pop((turn.link.id, origin.message_id), None)
                self._origin_channels.pop((turn.link.id, origin.message_id), None)
            turn.cancelled_waiting = turn.cancelled_waiting[1:]
        for row in turn.stop_commands:
            guard = self._guard(turn, repair=True)
            await guard.prepare()
            guard.current()
            await self.services.outbox.inbox.finish_route(row)
            guard.current()
        turn.stop_commands.clear()

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
            except Exception:
                if not reported:
                    logger.warning("telegram_bots chat_observation_held")
                    reported = True
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
        except Exception:
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
            logger.warning("telegram_bots chat_dispatch_result_held")
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
                        display = self._spawn(self._display_loop(turn, turn.presentation), channel=turn.channel)
                if watcher is None or watcher.done():
                    watcher = self._spawn(self.observer.watch(record, turn.link, on_preview=lambda value: self._preview(turn, value), on_line=lambda key, values: self._activity(turn, key, values), turn_done=turn.done), channel=turn.channel)
                if turn.stop_requested:
                    await self._observe(turn, lambda: self._request_stop(turn))
                    if turn.done.is_set():
                        return
                run = await self._observe(turn, lambda: self.observer.read_run(record, turn.link))
                if not self._current(turn):
                    return
                failure = classify_creation_refusal(turn.refusal, channel_execution_scope(record)) if turn.refusal is not None else None
                if run is not None and run.row.get("status") in {"success", "error", "timeout", "interrupted"}:
                    failure = classify_run(run, channel_execution_scope(record))
                    stopped = turn.stop_requested and run.row.get("status") == "interrupted"
                    if stopped or failure is None:
                        if await self._observe(turn, lambda: self._pause_ui(turn, final=True)):
                            waiting = bool(turn.cancelled_waiting) if turn.waiting_dropped else bool(self.queue.waiting_count(turn.link, turn.topic))
                            identity = await self._observe(turn, lambda: self.observer.plan_final(record, turn.link, stopped=stopped, waiting=waiting))
                            if identity is not None:
                                if stopped:
                                    self._drop_waiting(turn)
                                await self._observe(turn, lambda: self._complete(turn))
                                return
                        # A stopped stream still requires acknowledged exact
                        # cancellation and unavailable(cancelled) source proof.
                        if stopped:
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
                                turn.refusal = None
                                turn.stop_status = None
                                turn.stop_marker_checked = False
                                turn.capacity_waiting = False
                                if turn.cancel_future:
                                    raise ValueError("chat-turn-stop-before-retry-marker") from None

                            try:
                                if turn.refusal is None:
                                    turn.record = await self.services.publications.retry(old, turn.link, on_reserved=pin)
                                else:
                                    turn.record = await self.services.publications.retry_refused(old, turn.link, turn.refusal, on_reserved=pin)
                            except asyncio.CancelledError:
                                raise
                            except Exception:
                                if not self._current(turn) or turn.record.publication_id == old.publication_id:
                                    raise
                                # The successor was positively handed off before
                                # its creating result became uncertain. Observe
                                # only that identity; never redispatch it.
                                logger.warning("telegram_bots chat_retry_result_held")
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
                            await self._observe(turn, lambda: self._publication_outcome(turn, failure.text_key, run=run, refusal=turn.refusal if run is None else None))
                            await self._observe(turn, lambda: self._complete(turn))
                            return
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
            if turn.stop_status is None:
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
                if turn.stop_status == "refused":
                    for row in turn.stop_commands:
                        await turn.channel.send_text("stop.failed", source_id=f"chat-stop-refused:{row.chat_id}:{row.message_id}", inbox_rows=[row], finish_route=True, thread=turn.topic, _captured_link=turn.link)
                    turn.stop_commands.clear()
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
            original = self._turns.get((link.id, topic))
            if self._closed or channel in self._retired or not await channel._input_current(link) or not channel._callback_current(link):
                return
            row, created = await await_drained(self.services.outbox.inbox.accept(link, update, sanitized_input={"command": "stop"}))
            if self._closed or channel in self._retired or not channel._callback_current(link):
                return
            if row.route_complete or not created:
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
                if row.kind == "cancel_wait":
                    if turn.record is None or turn.capacity_waiting is not True or row.target_kind != "attempt" or row.target_id != turn.record.turn_id:
                        raise CallbackRefused() from None
                else:
                    view = turn.presentation
                    if view is None or not await view.matches_callback(row):
                        raise CallbackRefused() from None
                    if not self._current(turn) or turn.presentation is not view or row.target_kind != "draft" or row.target_id != str(view.draft_id):
                        raise CallbackRefused() from None
                await self._stop_turn(turn, stop_id="callback:" + row.id)
                return
            if row.kind != "retry_message":
                raise CallbackRefused() from None

            def current():
                return not self._closed and channel not in self._retired and channel._callback_current(link)

            guard = _Guard(self.services, channel, link, current, repair=True)
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
            for origin in stage.origins:
                self._origins[(link.id, origin.message_id)] = origin
                self._origin_channels[(link.id, origin.message_id)] = channel
            if result == "run":
                self._start(channel, link, inbound)

    async def recover_startup(self):
        """One bounded frozen input cohort while the host still holds ingress.

        This is a fixed restart disposition, never model retry or a fabricated
        publication. Unknown/unrouted/incomplete groups stay retained.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 30
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

        if not phase():
            return
        # Retained SQL may settle after D. Its result never re-arms the phase.
        try:
            page = await await_drained(host.outbox.inbox.prepublication_page(limit=32, max_origins=64))
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning("telegram_bots chat_startup_page_held")
            return
        if not phase():
            return
        cohort = tuple(page.entries)
        for entry in cohort:
            if not phase():
                return
            if entry.hold_reason is not None or not entry.origins:
                continue
            try:
                await self._recover_input(entry, phase)
            except asyncio.CancelledError:
                raise
            except Exception:
                # A stale or unreadable cohort member cannot admit itself, and
                # cannot suppress another independent frozen input's outcome.
                logger.warning("telegram_bots chat_startup_input_held")

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
            return phase() and host.channel(link.id) is channel and channel._same_scope(captured, channel.link) and channel.transport_revision == captured.token_revision and channel.application.running and not channel._stopping

        guard = _Guard(host, channel, captured, current, repair=True)
        if not current():
            return
        await host.outbox.plan_input_outcome(
            captured,
            source_id=f"chat-restart:{captured.chat_id}:{captured.binding_generation}:" + (entry.input_owner_callback_id or str(entry.origins[0].message_id)),
            source_ref={"key": "chat.restarted", "values": {}},
            keyboard=self._button("retry_message", entry.input_owner_callback_id or str(entry.origins[0].message_id), target_kind="request"),
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

    def typing_allowed(self, channel, *, topic):
        topic = None if topic in (None, 1) else topic
        turn = self._turns.get((channel.link.id, topic))
        view = None if turn is None else turn.presentation
        return not (turn is not None and turn.channel is channel and self._current(turn) and view is not None and not view.closed and view.mode == "draft" and view.started and not view.paused)

    def preview_allowed(self, channel, item):
        return any(turn.channel is channel and self._current(turn) and turn.presentation is not None and turn.presentation.allows_placeholder(item) is True for turn in tuple(self._turns.values()))

    def _retire_now(self, channel):
        self._retired.add(channel)
        retired = []
        for key, turn in tuple(self._turns.items()):
            if turn.channel is not channel:
                continue
            turn.active = False
            turn.done.set()
            if turn.record is not None and self._observer is not None:
                self.observer.retire(turn.record)
            self.queue.cancel_wait(turn.link, turn.topic)
            self.queue.turn_ended(turn.link, turn.topic)
            self._turns.pop(key, None)
            retired.append(turn)
        for key, owner in tuple(self._origin_channels.items()):
            if owner is channel:
                self._origin_channels.pop(key, None)
                self._origins.pop(key, None)
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
                for result in results:
                    if isinstance(result, BaseException):
                        raise result

            self._stop_task = asyncio.create_task(settle(), name="telegram-chat-stop")
        await await_drained(self._stop_task)
