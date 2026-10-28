"""Exact-publication advisory observation and owner-authorized Stop."""

from __future__ import annotations

import asyncio
import json
import weakref
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from app.channels.execution import ChannelExecutionScope
from app.channels.execution_client import ChannelExecutionFacade
from app.telegram_bots.inbox import publication_identity
from app.telegram_bots.stopped_preview import StoppedSegment, _issue_shown_preview, _issue_stopped_preview
from deerflow.runtime.runs.preview import PREVIEW_SPAN_KEY, PreviewProtocolError, PreviewSpan, text_digest
from deerflow.utils.file_io import await_drained


def _refuse():
    raise PreviewProtocolError() from None


def _identity(record):
    return json.dumps(publication_identity(record), sort_keys=True, separators=(",", ":"))


@dataclass
class _State:
    record: object
    link: object
    key: str
    active: bool = True
    task: asyncio.Task | None = None
    attempt: object = None
    facade: object = None
    stream: object = None
    stop_id: str | None = None
    stop_attempted: bool = False
    stop_accepted: bool = False
    stop_decisions: dict[str, str] = field(default_factory=dict)
    stop_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    sequence: int = 0
    prefixes: dict = field(default_factory=dict)
    issued: object = field(default_factory=weakref.WeakValueDictionary)
    seen: dict = field(default_factory=dict)
    shown: dict = field(default_factory=dict)
    cancel_guard: object = None
    watching: bool = False
    tool_chunks: dict = field(default_factory=dict)
    activities: dict = field(default_factory=dict)
    cursor: str | None = None


@dataclass(frozen=True)
class _Admission:
    observer: object
    state: _State
    original: object

    def current(self, operation):
        return self.observer._current(self.state) and self.original.current(operation) is True

    async def authorize(self, operation):
        self.observer._check(self.state)
        await self.original.authorize(operation)
        self.observer._check(self.state)


@dataclass(frozen=True)
class _Guard:
    observer: object
    state: _State
    original: object

    @property
    def captured_link(self):
        return self.original.captured_link

    async def prepare(self):
        self.observer._check(self.state)
        await self.original.prepare()
        self.observer._check(self.state)

    async def check(self, session):
        self.observer._check(self.state)
        result = await self.original.check(session)
        self.observer._check(self.state)
        return result


_PREVIEW_ISSUER = object()


@dataclass(frozen=True, init=False)
class ObservedPreview:
    span: PreviewSpan
    text: str
    _state: _State

    def __init__(self, issuer, state, span, text):
        if issuer is not _PREVIEW_ISSUER:
            _refuse()
        object.__setattr__(self, "span", span)
        object.__setattr__(self, "text", text)
        object.__setattr__(self, "_state", state)

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self


class RunObserver:
    def __init__(self, *, coordinator, http_owner, capture_attempt, is_current):
        self.coordinator, self.http_owner, self.capture_attempt, self.is_current = coordinator, http_owner, capture_attempt, is_current
        self._states, self._operations = {}, set()
        self._closed = False

    def _current(self, state):
        return not self._closed and state.active and self.is_current(state.record) is True

    def _check(self, state):
        if not self._current(state):
            _refuse()

    def _update_record(self, state, fresh):
        if _identity(fresh) != state.key or state.record.run_id is not None and fresh.run_id not in {None, state.record.run_id}:
            _refuse()
        # A concurrent pre-bind writer may return an older observation. It
        # cannot erase a positive exact association already received here.
        if fresh.run_id is not None or state.record.run_id is None:
            state.record = fresh

    def _state(self, record, link):
        key = _identity(record)
        state = self._states.get(key)
        if state is None:
            scope = record.scope
            if (scope.link_id, scope.owner_user_id, scope.binding_generation, scope.chat_id, scope.endpoint_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id, self.http_owner.endpoint_id):
                _refuse()
            state = self._states[key] = _State(record, link, key)
        elif state.link != link:
            _refuse()
        self._check(state)
        return state

    async def _facade(self, state):
        self._check(state)
        if state.facade is None:
            scope = state.record.scope
            attempt = await await_drained(self.capture_attempt(state.link, thread_id=scope.thread_id, endpoint_id=scope.endpoint_id))
            self._check(state)
            if (attempt.thread_id, attempt.endpoint_id) != (scope.thread_id, scope.endpoint_id):
                _refuse()
            state.attempt = attempt
            if attempt.cancel_guard is not None:
                state.cancel_guard = _Guard(self, state, attempt.cancel_guard)
            execution = ChannelExecutionScope(state.record.publication_id, scope.owner_user_id, scope.connection_id, str(scope.chat_id), scope.thread_id, scope.endpoint_id, scope.assistant_id, scope.agent_name)
            state.facade = ChannelExecutionFacade(self.http_owner, scope=execution, admission=_Admission(self, state, attempt.admission), capture_output=True, capture_inventory=True, inventory_document_version=2)
        return state.facade

    async def watch(self, record, current_link, *, on_preview, on_line, turn_done):
        state = self._state(record, current_link)
        if state.watching:
            _refuse()
        state.watching = True
        state.task = asyncio.current_task()
        try:
            while state.record.run_id is None:
                self._check(state)
                if turn_done.is_set():
                    state.stop_decisions.pop(state.stop_id, None)
                    state.stop_id = None
                    return None
                located = await self.coordinator.locate_run(state.record, state.link, max_pages=8)
                self._check(state)
                if located.state == "matched":
                    self._update_record(state, located.record)
                    break
                try:
                    await asyncio.wait_for(turn_done.wait(), 1)
                except TimeoutError:
                    pass
            facade = await self._facade(state)
            if state.stop_id is not None and not state.stop_attempted:
                async with state.stop_lock:
                    if state.stop_id is not None and not state.stop_attempted:
                        await self._apply_stop(state)
            self._check(state)
            state.stream = facade.join_run(state.record.run_id, after_event_id=state.cursor)
            async for part in state.stream:
                self._check(state)
                if state.stop_accepted:
                    continue
                if part.event in {"messages", "messages-tuple"}:
                    preview = self._preview(state, part.data)
                    if preview is not None:
                        await on_preview(preview)
                    for key, values in self._activity(state, part.data):
                        self._check(state)
                        await on_line(key, values)
                cursor = part.id
                if cursor is not None:
                    if type(cursor) is not str or not cursor or len(cursor) > 256 or not cursor.isprintable():
                        _refuse()
                    state.cursor = cursor
                # Activity is independent advisory data, never output proof.
            self._check(state)
            await on_line(None, {})
            self._check(state)
            return state.record.run_id
        except asyncio.CancelledError:
            state.active = False
            raise
        finally:
            state.watching = False
            if state.stream is not None:
                await await_drained(state.stream.aclose())
                state.stream = None
            if not state.active and self._states.get(state.key) is state:
                self._states.pop(state.key)

    @staticmethod
    def _activity_line(name, args):
        # Tool activity is a fixed catalog mapping. Command lines, search
        # queries, full paths and returned tool contents never enter this UI.
        if name in {"ask_clarification", "report_work", "propose_assignment", "show_web_page"}:
            return None
        simple = {
            "ls": "list",
            "glob": "list",
            "grep": "list",
            "bash": "command",
            "web_search": "search",
            "image_search": "search",
            "task": "subtask",
            "batch_task": "subtask",
            "present_files": "present",
            "read_work_context": "work",
        }
        if name in simple:
            return "activity." + simple[name], {}
        if name.startswith("browser_"):
            return "activity.browser", {}
        if name in {"read_file", "write_file", "str_replace", "view_image"}:
            value = args.get("path", args.get("file_path", args.get("filename")))
            if type(value) is str:
                basename = value.replace("\\", "/").rsplit("/", 1)[-1]
                if basename and basename.isprintable() and len(basename) <= 80:
                    return "activity." + {"read_file": "read", "write_file": "write", "str_replace": "edit", "view_image": "image"}[name], {"file": basename}
        if name in {"web_fetch", "web_capture"}:
            try:
                value = args.get("url")
                url = urlsplit(value) if type(value) is str and value.isprintable() else None
                if url is not None and url.scheme in {"http", "https"} and url.hostname and len(url.hostname) <= 253:
                    return "activity.fetch", {"host": url.hostname}
            except ValueError:
                pass
        return "activity.other", {}

    def _activity(self, state, data):
        if not isinstance(data, (tuple, list)) or len(data) != 2 or type(data[0]) is not dict or type(data[1]) is not dict:
            return ()
        message, metadata = data
        tags = metadata.get("tags", ())
        if not isinstance(tags, (tuple, list)) or any(type(tag) is str and tag.startswith(("subagent:", "middleware:")) for tag in tags):
            return ()
        if message.get("type") in {"tool", "ToolMessage"}:
            identity = message.get("tool_call_id")
            if type(identity) is str and identity in state.activities:
                state.activities.pop(identity)
                return (next(reversed(state.activities.values())) if state.activities else (None, {}),)
            return ()
        if message.get("type") not in {"ai", "AIMessageChunk"}:
            return ()
        updates = []
        chunks = message.get("tool_call_chunks")
        calls = chunks if type(chunks) is list and chunks else message.get("tool_calls", [])
        if type(calls) is not list:
            return ()
        for call in calls:
            if type(call) is not dict:
                continue
            identity, name, args = call.get("id"), call.get("name"), call.get("args", {})
            if calls is chunks:
                index = call.get("index")
                message_id = message.get("id")
                if type(index) is not int or type(message_id) is not str:
                    continue
                key = message_id, index
                saved = state.tool_chunks.get(key, (None, "", ""))
                identity = identity or saved[0]
                name = saved[1] + (name or "") if type(name) in {str, type(None)} else ""
                raw = saved[2] + args if type(args) is str else ""
                if len(raw) > 4096 or len(state.tool_chunks) >= 128 and key not in state.tool_chunks:
                    continue
                state.tool_chunks[key] = identity, name, raw
                try:
                    args = json.loads(raw)
                except (ValueError, TypeError):
                    continue
            if type(identity) is not str or not identity or type(name) is not str or not name or type(args) is not dict:
                continue
            line = self._activity_line(name, args)
            if line is not None and state.activities.get(identity) != line:
                state.activities[identity] = line
                updates.append(line)
        return tuple(updates)

    def _preview(self, state, data):
        if not isinstance(data, (tuple, list)) or len(data) != 2 or type(data[0]) is not dict or type(data[1]) is not dict or PREVIEW_SPAN_KEY not in data[1]:
            return None
        try:
            span = PreviewSpan.from_dict(data[1][PREVIEW_SPAN_KEY])
            record = state.record
            if (span.owner_user_id, span.thread_id, span.run_id, span.publication_id) != (record.scope.owner_user_id, record.scope.thread_id, record.run_id, record.publication_id):
                _refuse()
            message = data[0]
            text = message.get("content")
            if message.get("type") not in {"ai", "AIMessageChunk"} or message.get("id") != span.message_id or type(text) is not str or text_digest(text) != span.text_sha256:
                _refuse()
            if span.sequence <= state.sequence:
                if state.seen.get(span.sequence) != span:
                    _refuse()
                return None
            if span.sequence != state.sequence + 1:
                _refuse()
            key = (span.invocation_id, span.model_call_id, span.message_id)
            previous = state.prefixes.get(key, "")
            prefix = previous + text
            if span.offset != len(previous) or span.prefix_sha256 != text_digest(prefix):
                _refuse()
            state.prefixes[key], state.sequence = prefix, span.sequence
            state.seen[span.sequence] = span
            if len(state.seen) > 256:
                state.seen.pop(next(iter(state.seen)))
            preview = ObservedPreview(_PREVIEW_ISSUER, state, span, prefix)
            state.issued[id(preview)] = preview
            return preview
        except Exception:
            _refuse()

    def record_shown(self, preview, *, mode, part_id, anchor_message_id=None, message_id=None, draft_id=None, part_index=0, source_start=0, source_end=None, text_prefix=None):
        if type(preview) is not ObservedPreview:
            _refuse()
        state = preview._state
        self._check(state)
        if state.issued.get(id(preview)) is not preview or state.stop_accepted:
            _refuse()
        text = preview.text if text_prefix is None else text_prefix
        if type(text) is not str or not preview.text.startswith(text):
            _refuse()
        end = len(text) if source_end is None else source_end
        if type(source_start) is not int or type(end) is not int or source_start < 0 or end < source_start or end > len(text):
            _refuse()
        text = text[source_start:end]
        segment = StoppedSegment(
            preview.span.model_call_id, preview.span.message_id, preview.span.sequence, text, text_digest(text), mode, anchor_message_id if anchor_message_id is not None else message_id, draft_id, part_index, part_id, source_start, end
        )
        previous = state.shown.get(part_id)
        if previous is not None and previous.sequence > segment.sequence:
            _refuse()
        state.shown[part_id] = segment

    def run_id(self, thread_id, publication_id):
        matches = [state.record.run_id for state in self._states.values() if state.active and state.record.scope.thread_id == thread_id and state.record.publication_id == publication_id]
        return matches[0] if len(matches) == 1 else None

    async def read_run(self, record, current_link):
        """Fresh exact-run evidence, without inferring completion from SSE EOF."""
        state = self._state(record, current_link)
        if state.record.run_id is None:
            return None
        facade = await self._facade(state)
        run = await facade.lookup_run()
        if run is not None and run.row["run_id"] != state.record.run_id:
            _refuse()
        self._check(state)
        return run

    def stop_outcome(self, record, current_link, *, stop_id):
        """Read an original Stop's local decision; grant no new admission."""
        if type(stop_id) is not str or not stop_id or not stop_id.isprintable():
            _refuse()
        state = self._states.get(_identity(record))
        if state is None or not self._current(state):
            return None
        if state.link != current_link:
            _refuse()
        return state.stop_decisions.get(stop_id)

    async def request_stop(self, record, current_link, *, stop_id):
        state = self._state(record, current_link)
        if type(stop_id) is not str or not stop_id or not stop_id.isprintable():
            _refuse()

        async def perform():
            async with state.stop_lock:
                self._check(state)
                decision = state.stop_decisions.get(stop_id)
                if decision is not None:
                    if decision != "pending" or state.record.run_id is None or state.stop_attempted:
                        return decision
                    return await self._apply_stop(state)
                facade = await self._facade(state)
                if state.cancel_guard is None:
                    _refuse()
                self._check(state)
                fresh = await self.coordinator.inbox.request_publication_stop(state.record, stop_id=stop_id, guard=state.cancel_guard)
                self._check(state)
                self._update_record(state, fresh)
                state.stop_id = stop_id
                state.stop_decisions[stop_id] = "pending"
                state.stop_attempted, state.stop_accepted = False, False
                if state.record.run_id is None:
                    return "pending"
                return await self._apply_stop(state, facade=facade)

        task = asyncio.create_task(perform(), name="telegram-exact-stop")
        self._operations.add(task)
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            task.cancel()
            await await_drained(asyncio.gather(task, return_exceptions=True))
            raise
        finally:
            self._operations.discard(task)

    async def _apply_stop(self, state, *, facade=None):
        self._check(state)
        stop_id = state.stop_id
        if stop_id is None:
            _refuse()
        decision = state.stop_decisions.get(stop_id)
        if decision not in {None, "pending"}:
            return decision
        if state.stop_attempted:
            return "pending"
        facade = facade or await self._facade(state)
        state.stop_attempted = True
        try:
            outcome = await facade.cancel_run(state.record.run_id)
            self._check(state)
            if outcome == "refused":
                fresh = await self.coordinator.inbox.clear_publication_stop(state.record, stop_id=state.stop_id, guard=state.cancel_guard)
                self._check(state)
                self._update_record(state, fresh)
                state.stop_id, state.stop_attempted = None, False
            elif outcome == "requested":
                state.stop_accepted = True
            state.stop_decisions[stop_id] = outcome
            return outcome
        except asyncio.CancelledError:
            state.stop_decisions[stop_id] = "unknown"
            raise
        except Exception:
            state.stop_decisions[stop_id] = "unknown"
            return "unknown"

    async def freeze_stopped(self, record, current_link):
        state = self._state(record, current_link)
        if not state.stop_accepted or state.record.run_id is None:
            _refuse()
        facade = await self._facade(state)
        output = await facade.get_run_output(state.record.run_id)
        self._check(state)
        if output is None or output.output.state != "unavailable" or output.output.reason != "cancelled":
            _refuse()
        return _issue_stopped_preview(
            publication_identity=publication_identity(state.record), run_id=state.record.run_id, stop_id=state.stop_id, segments=tuple(state.shown.values()), check_current=lambda: self._current(state) and state.stop_accepted
        )

    def freeze_shown(self, record, current_link):
        state = self._state(record, current_link)
        if state.record.run_id is None:
            _refuse()
        return _issue_shown_preview(publication_identity=publication_identity(state.record), run_id=state.record.run_id, segments=tuple(state.shown.values()), check_current=lambda: self._current(state))

    async def plan_final(self, record, current_link, *, stopped=False, waiting=False):
        state = self._state(record, current_link)
        if type(stopped) is not bool or type(waiting) is not bool or state.record.run_id is None:
            _refuse()
        facade = await self._facade(state)
        output = await facade.get_run_output(state.record.run_id)
        self._check(state)
        if output is None or output.output.state == "pending":
            return None
        guard = _Guard(self, state, state.attempt.read_guard)
        existing = await self.coordinator.outbox.read_display_primary(state.record, output=output, guard=guard)
        self._check(state)
        if existing is not None:
            return existing
        if self.coordinator.file_output_planner is not None and output.inventory.state == "matched" and output.inventory.document.coverage == "complete" and output.inventory.document.items:
            identity = await self.coordinator.file_output_planner(state.record, current_link, output, guard=guard, shown=self.freeze_shown(record, current_link))
            self._check(state)
            if identity is not None:
                return identity
        if output.output.state == "complete":
            # A 202 Stop response is only a request. The normal terminal pair
            # may have won the backend race and remains the sole final source.
            return await self.coordinator.outbox.plan_streamed_publication_reply(state.record, output=output, shown=self.freeze_shown(record, current_link), guard=guard)
        if stopped:
            if not state.stop_accepted or output.output.state != "unavailable" or output.output.reason != "cancelled":
                return None
            preview = _issue_stopped_preview(
                publication_identity=publication_identity(state.record), run_id=state.record.run_id, stop_id=state.stop_id, segments=tuple(state.shown.values()), check_current=lambda: self._current(state) and state.stop_accepted
            )
            return await self.coordinator.outbox.plan_stopped_publication_reply(state.record, output=output, preview=preview, guard=guard, waiting=waiting)
        return None

    def retire(self, record):
        state = self._states.get(_identity(record))
        if state is not None:
            state.active = False
            if state.stream is None and (state.task is None or state.task.done() or state.task is asyncio.current_task()):
                self._states.pop(state.key)
            if state.task is not None and not state.task.done() and state.task is not asyncio.current_task():
                state.task.cancel()

    async def drain(self):
        self._closed = True
        for state in self._states.values():
            state.active = False
            if state.task is not None and not state.task.done() and state.task is not asyncio.current_task():
                state.task.cancel()
        for task in self._operations:
            task.cancel()
        tasks = [state.task for state in self._states.values() if state.task is not None and state.task is not asyncio.current_task()] + list(self._operations)
        if tasks:
            await await_drained(asyncio.gather(*tasks, return_exceptions=True))
        for state in self._states.values():
            if state.stream is not None:
                await await_drained(state.stream.aclose())
        self._states.clear()
