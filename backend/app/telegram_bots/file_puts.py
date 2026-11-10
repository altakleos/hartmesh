"""Durable direct file writes; only the live owner repeats a transient refusal."""

from __future__ import annotations

import asyncio
import hashlib
import io
import re
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

from sqlalchemy import select

from app.channels.inbound_leases import InboundFileLease
from app.telegram_bots.acts import _snapshot
from app.telegram_bots.callbacks import CALLBACK_METADATA_KEYS, CallbackRefused
from app.telegram_bots.inbox import InboxRow, chat_input_parts, validate_chat_file
from app.telegram_bots.instance_file_authority import prepare_file_write
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.producer import keyboard_cleanup_row
from deerflow.agent_instances.agent_files import AGENT_FILE_REFUSALS, AgentFileRefusal, AgentFileResult
from deerflow.agent_instances.contract import ResidentFilesBusy
from deerflow.persistence.agent_instances.model import AgentInstanceRow
from deerflow.persistence.spaces.model import SpaceRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow
from deerflow.runtime.keyed_lock import AsyncKeyedLockTable
from deerflow.utils.file_io import await_drained

_BODY = {"name", "size", "sha256", "path", "file_id", "file_unique_id", "message_id"}


def file_act_id(link, file):
    return hashlib.sha256(f"{link.id}:{file['file_unique_id']}:{file['message_id']}".encode()).hexdigest()[:32]


def child_operation(identity, label):
    return hashlib.sha256(f"{identity}:{label}".encode()).hexdigest()[:32]


def _target(access):
    instance = access.instance
    return dict(
        instance_id=instance.id,
        principal_kind=instance.principal.kind,
        principal_id=instance.principal.subject_id,
        instance_generation=instance.generation,
        definition_revision=instance.definition_revision,
        home_id=instance.home_id,
        home_generation=access.home_generation,
    )


def _fallback(file):
    extension = file["filename"].rsplit(".", 1)[-1] if "." in file["filename"] else ""
    extension = extension if re.fullmatch(r"[A-Za-z0-9]{1,16}", extension) else ("jpg" if file["type"] == "image" else "bin")
    return f"telegram-{file['message_id']}.{extension}"


def _file_result(value, *, max_bytes=None):
    if (
        type(value) is not dict
        or set(value) != {"path", "size", "sha256", "deduplicated"}
        or type(value["size"]) is not int
        or (value["size"] < 0 or max_bytes is not None and value["size"] > max_bytes)
        or type(value["sha256"]) is not str
        or re.fullmatch(r"[0-9a-f]{64}", value["sha256"]) is None
        or type(value["path"]) is not str
        or not value["path"].startswith("uploads/")
        or value["path"][8:] in {"", ".", ".."}
        or "/" in value["path"][8:]
        or "\\" in value["path"]
        or type(value["deduplicated"]) is not bool
    ):
        raise ValueError("file-result-conflict") from None


def _file_journal(row, *, max_bytes=None):
    """Closed persisted shape, independent of current service/reader authority."""
    if row.kind != "file_put" or type(row.id) is not str or re.fullmatch(r"[0-9a-f]{32}", row.id) is None:
        raise ValueError("file-act-conflict") from None
    continuation = row.continuation
    if type(continuation) is not dict or set(continuation) != {"version", "file", "labels", "presses", "target"} or type(continuation["version"]) is not int or continuation["version"] != 1:
        raise ValueError("file-act-conflict") from None
    target = continuation["target"]
    if (
        type(target) is not dict
        or set(target) != {"instance_id", "principal_kind", "principal_id", "instance_generation", "definition_revision", "home_id", "home_generation"}
        or any(type(target[key]) is not str or re.fullmatch(r"[0-9a-f]{32}", target[key]) is None for key in ("instance_id", "home_id"))
        or target["instance_id"] != row.instance_id
        or target["principal_kind"] != "nonhuman"
        or target["principal_id"] != "agent:" + row.instance_id
        or type(target["definition_revision"]) is not str
        or re.fullmatch(r"[0-9a-f]{64}", target["definition_revision"]) is None
        or any(type(target[key]) is not int or target[key] < 1 for key in ("instance_generation", "home_generation"))
    ):
        raise ValueError("file-act-conflict") from None
    labels, presses = continuation["labels"], continuation["presses"]
    if (
        type(labels) is not list
        or not labels
        or labels[0] != "initial"
        or any(type(label) is not str for label in labels)
        or len(labels) != len(set(labels))
        or type(presses) is not list
        or any(
            type(press) is not dict
            or set(press) != {"callback_id", "message_id", "topic"}
            or type(press["callback_id"]) is not str
            or re.fullmatch(r"[0-9a-f]{32}", press["callback_id"]) is None
            or type(press["message_id"]) is not int
            or press["message_id"] <= 0
            or press["topic"] is not None
            and (type(press["topic"]) is not int or press["topic"] <= 0)
            for press in presses
        )
    ):
        raise ValueError("file-act-conflict") from None
    if [label for label in labels[1:] if label != "rename"] != [f"retry:{number}" for number in range(2, len(presses) + 2)] or len({press["callback_id"] for press in presses}) != len(presses):
        raise ValueError("file-act-conflict") from None
    file = validate_chat_file(continuation["file"])
    if file["telegram_bot_id"] != row.bot_id or row.message_id != file["message_id"] or type(row.body) is not dict or set(row.body) != _BODY:
        raise ValueError("file-act-conflict") from None
    if any(type(row.body[key]) is not type(file[key]) or row.body[key] != file[key] for key in ("file_id", "file_unique_id", "message_id")) or row.body["name"] != (
        _fallback(file) if "rename" in continuation["labels"] else file["filename"]
    ):
        raise ValueError("file-act-conflict") from None
    expected_ops = [row.id] + [child_operation(row.id, label) for label in labels[1:]]
    if row.operation_ids != expected_ops or type(row.attempts) is not int or row.attempts < 0:
        raise ValueError("file-act-conflict") from None
    if any(getattr(row, key) is not None for key in ("work_id", "request_id", "stop_dispatch", "cancel_requested_at")) or row.state not in {"draft", "pending", "waiting", "done", "refused", "uncertain"}:
        raise ValueError("file-act-conflict") from None
    if (
        row.state in {"draft", "pending", "done"}
        and row.reason is not None
        or row.state == "refused"
        and row.reason not in {*AGENT_FILE_REFUSALS, "storage-busy"}
        or row.state == "waiting"
        and row.reason not in {"file-write-failed", "storage-busy", "invalid-name"}
        or row.state == "uncertain"
        and row.reason not in {"file-write-failed", "download-failed", "interrupted"}
    ):
        raise ValueError("file-act-conflict") from None
    if row.attempts == 0 and (row.state != "draft" or row.first_attempt_at is not None) or row.attempts > 0 and row.first_attempt_at is None:
        raise ValueError("file-act-conflict") from None
    if row.state == "done":
        _file_result(row.result, max_bytes=max_bytes)
        if row.reason is not None or any(type(row.body[key]) is not type(row.result[key]) or row.body[key] != row.result[key] for key in ("path", "sha256", "size")):
            raise ValueError("file-act-conflict") from None
    elif row.result is not None or row.body["path"] is not None or row.body["sha256"] is not None or type(row.body["size"]) is not type(file["size"]) or row.body["size"] != file["size"]:
        raise ValueError("file-act-conflict") from None
    return file


def settled_file_journal(row):
    """Only known terminal file metadata is safe for the age-based sweep."""
    if row.state not in {"done", "refused"}:
        return False
    try:
        _file_journal(row)
    except (ValueError, TypeError, KeyError, AttributeError):
        return False
    return True


class FilePuts:
    """One journal per source file, with no recovery worker or retained bytes.

    SQL writers fence every transition. The process lock spans download, bounded
    retries and completion; cancellation drains the whole unit and its lease.
    """

    def __init__(self, callbacks, inbox, *, config):
        self.callbacks, self.inbox, self.config = callbacks, inbox, config
        self._locks = AsyncKeyedLockTable()
        self._time, self._sleep = time.monotonic, asyncio.sleep

    def _scope(self, link):
        return dict(link_id=link.id, bot_id=link.bot_id, owner_user_id=link.owner_user_id, binding_generation=link.binding_generation, telegram_user_id=link.telegram_user_id, chat_id=link.chat_id, instance_id=link.agent_ref)

    async def _row(self, writer, link, identity, access, *, lock=True):
        row = await writer.get(TelegramActRow, identity, with_for_update=lock, populate_existing=True)
        if row is None or row.kind != "file_put" or link.agent_kind != "instance" or any(getattr(row, key) != value for key, value in self._scope(link).items()):
            raise ValueError("file-act-conflict") from None
        file = _file_journal(row, max_bytes=self.config.max_inbound_bytes)
        if file_act_id(link, file) != row.id:
            raise ValueError("file-act-conflict") from None
        target, expected = row.continuation["target"], _target(access)
        if any(type(target[key]) is not type(value) or target[key] != value for key, value in expected.items()):
            raise ValueError("file-target-changed") from None
        # A captured target cannot turn into a new Home while a journal read
        # waits for its writer. This read takes no instance lock after the link.
        actual = (
            await writer.execute(
                select(AgentInstanceRow.principal_id, AgentInstanceRow.generation, AgentInstanceRow.definition_revision, AgentInstanceRow.home_id, SpaceRow.generation)
                .join(SpaceRow, SpaceRow.id == AgentInstanceRow.home_id)
                .where(AgentInstanceRow.id == access.instance.id)
            )
        ).one_or_none()
        if actual is None or tuple(actual) != (target["principal_id"], target["instance_generation"], target["definition_revision"], target["home_id"], target["home_generation"]):
            raise ValueError("file-target-changed") from None
        await access.check_receipt(writer)
        return row

    async def _prepare(self, link, origin, file, access):
        file = validate_chat_file(file)
        identity = file_act_id(link, file)
        async with self.callbacks.writer(link, access.guard) as writer:
            saved = await writer.get(TelegramActRow, identity, with_for_update=True, populate_existing=True)
            if saved is not None:
                row = await self._row(writer, link, identity, access)
                if row.continuation["file"] != file:
                    raise ValueError("file-act-conflict") from None
                return _snapshot(row)
            mark = await writer.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id), with_for_update=True, populate_existing=True)
            if mark is None or (mark.link_id, mark.chat_id, mark.binding_generation, mark.route_kind, mark.route_target_id) != (link.id, link.chat_id, link.binding_generation, "turn", link.agent_ref) or mark.route_complete:
                raise ValueError("file-act-conflict") from None
            self.inbox._publication_snapshot_matches(mark, origin)
            actual = InboxRow.from_row(mark)
            _text, files = chat_input_parts(actual, bot_id=link.bot_id)
            if file not in files or file["message_id"] != mark.message_id:
                raise ValueError("file-act-conflict") from None
            now = datetime.now(UTC)
            row = TelegramActRow(
                id=identity,
                **self._scope(link),
                message_id=file["message_id"],
                kind="file_put",
                body=dict(name=file["filename"], size=file["size"], sha256=None, path=None, file_id=file["file_id"], file_unique_id=file["file_unique_id"], message_id=file["message_id"]),
                state="draft",
                operation_ids=[identity],
                attempts=0,
                first_attempt_at=None,
                continuation=dict(version=1, file=file, labels=["initial"], presses=[], target=_target(access)),
                created_at=now,
                updated_at=now,
            )
            writer.add(row)
            await writer.flush()
            return _snapshot(row)

    async def _transition(self, link, captured, access, *, state, reason=None, result=None, begin=False, rename=False):
        async with self.callbacks.writer(link, access.guard) as writer:
            row = await self._row(writer, link, captured.id, access)
            if row.state == "done":
                return _snapshot(row)
            if row.attempts != captured.attempts or row.operation_ids != list(captured.operation_ids) or row.state != captured.state:
                raise ValueError("file-attempt-stale") from None
            now = datetime.now(UTC)
            if begin:
                row.attempts += 1
                row.first_attempt_at = row.first_attempt_at or now
            if rename:
                if "rename" in row.continuation["labels"]:
                    raise ValueError("file-attempt-stale") from None
                row.continuation = {**row.continuation, "labels": [*row.continuation["labels"], "rename"]}
                row.operation_ids = [*row.operation_ids, child_operation(row.id, "rename")]
                row.body = {**row.body, "name": _fallback(row.continuation["file"])}
            if result is not None:
                if type(result) is not AgentFileResult:
                    raise ValueError("file-result-conflict") from None
                _file_result(asdict(result), max_bytes=self.config.max_inbound_bytes)
                row.result = asdict(result)
                row.body = {**row.body, "size": result.size, "sha256": result.sha256, "path": result.path}
            row.state, row.reason, row.updated_at = state, reason, now
            await writer.flush()
            return _snapshot(row)

    async def receipt_reader(self, link, *, authority, current):
        """Prepare external authority before a caller opens its metadata writer."""

        async def perform():
            access = await prepare_file_write(authority, link, current=current)
            return FileReceiptReader(self, access)

        return await await_drained(perform())

    async def composer_files(self, link, files, *, authority, current):
        """Project only completed original-target receipts into web-composer shape."""
        files = [validate_chat_file(file) for file in files]

        async def perform():
            reader = await self.receipt_reader(link, authority=authority, current=current)
            return await reader.composer_files(files)

        return await await_drained(perform())

    async def put(self, link, origin, file, *, authority, current, download, progress):
        """Save an actually admitted file, without starting a conversation or run."""
        file = validate_chat_file(file)

        async def perform():
            async with self._locks.hold(file_act_id(link, file)):
                access = await prepare_file_write(authority, link, current=current)
                row = await self._prepare(link, origin, file, access)
                if row.state in {"pending", "waiting"}:
                    return await self._transition(link, row, access, state="uncertain", reason="interrupted")
                if row.state != "draft":
                    return row
                return await self._run(link, row, access, authority, current, download, progress)

        return await await_drained(perform())

    async def retry(self, link, identity, *, callback_id, chat_id, message_id, message_thread_id, authority, current, download, progress):
        """An actual Send again press allocates one child before any file effect."""

        async def perform():
            async with self._locks.hold(identity):
                access = await prepare_file_write(authority, link, current=current)
                async with self.callbacks.writer(link, access.guard) as writer:
                    row = await self._row(writer, link, identity, access, lock=False)
                    previous = next((press for press in row.continuation["presses"] if press["callback_id"] == callback_id), None)
                    if previous is not None:
                        row = await self._row(writer, link, identity, access)
                        if chat_id != link.chat_id or previous != dict(callback_id=callback_id, message_id=message_id, topic=message_thread_id):
                            raise CallbackRefused() from None
                        # A redelivered press can read its result, never create
                        # another operation after a process interruption.
                        if row.state in {"draft", "pending", "waiting"}:
                            row.state, row.reason, row.updated_at = "uncertain", "interrupted", datetime.now(UTC)
                        await writer.flush()
                        return _snapshot(row)
                    # Lock ordering stays link, anchor/group, then Act. The
                    # process lock serializes the live file owner for this Act.
                    group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=access.guard, session=writer)
                    row = await self._row(writer, link, identity, access)
                    selected = group.selected
                    payload = {key: value for key, value in selected.payload.items() if key not in CALLBACK_METADATA_KEYS}
                    if row.state not in {"refused", "uncertain"} or selected.kind != "file_retry" or (selected.target_kind, selected.target_id) != ("act", identity) or payload != {"act_id": identity}:
                        raise CallbackRefused() from None
                    label = f"retry:{len(row.continuation['presses']) + 2}"
                    row.continuation = {**row.continuation, "labels": [*row.continuation["labels"], label], "presses": [*row.continuation["presses"], dict(callback_id=callback_id, message_id=message_id, topic=message_thread_id)]}
                    row.operation_ids = [*row.operation_ids, child_operation(identity, label)]
                    row.state, row.reason, row.updated_at = "draft", None, datetime.now(UTC)
                    await self.callbacks.consume_group(group, reason="done", session=writer)
                    writer.add(keyboard_cleanup_row(group.selected, owner_user_id=link.owner_user_id, now=datetime.now(UTC)))
                    await writer.flush()
                    captured = _snapshot(row)
                return await self._run(link, captured, access, authority, current, download, progress)

        return await await_drained(perform())

    async def _run(self, link, row, access, authority, current, download, progress):
        row = await self._transition(link, row, access, state="pending", begin=True)
        file = dict(row.continuation["file"])
        if file["size"] is not None and file["size"] > self.config.max_inbound_bytes:
            return await self._transition(link, row, access, state="refused", reason="file-too-large")

        async def check():
            await prepare_file_write(authority, link, current=current)
            access._current()

        await progress("file.saving")
        lease = await download(file, check)
        if not isinstance(lease, InboundFileLease):
            large = lease == "file.too_large_in"
            return await self._transition(link, row, access, state="refused" if large else "uncertain", reason="file-too-large" if large else "download-failed")
        try:
            if len(lease.data) > self.config.max_inbound_bytes:
                return await self._transition(link, row, access, state="refused", reason="file-too-large")
            with io.BytesIO(lease.data) as content:
                deadline, repeated_workflow, waiting = self._time() + self.config.retry_max_seconds, False, False
                while True:
                    try:
                        result = await access.apply(operation_id=row.operation_ids[-1], name=row.body["name"], content=content, size=len(lease.data))
                    except (AgentFileRefusal, ResidentFilesBusy) as exc:
                        reason = exc.reason
                        rename = reason == "invalid-name" and "rename" not in row.continuation["labels"]
                        transient = reason == "file-write-failed" and self._time() + self.config.retry_seconds <= deadline
                        workflow = reason == "storage-busy" and exc.extra.get("cause") == "workflow" and not repeated_workflow
                        if not (rename or transient or workflow):
                            return await self._transition(link, row, access, state="uncertain" if reason == "file-write-failed" else "refused", reason=reason)
                        row = await self._transition(link, row, access, state="waiting", reason=reason, rename=rename)
                        if transient and not waiting:
                            await progress("file.waiting")
                            waiting = True
                        if workflow:
                            repeated_workflow = True
                        if not rename:
                            await self._sleep(self.config.retry_seconds)
                        await check()
                        row = await self._transition(link, row, access, state="pending", begin=True)
                    else:
                        return await self._transition(link, row, access, state="done", result=result)
        finally:
            await lease.aclose()


@dataclass(frozen=True)
class FileReceiptReader:
    """Original-target journal reader for the owning metadata transaction."""

    journal: FilePuts
    access: object

    def check_link(self, link):
        self.journal.inbox._publication_guard_matches(self.access.guard, link=link)
        self.access._current()

    def guard(self, base=None):
        base = self.access.guard if base is None else base
        if not callable(getattr(base, "current", None)):
            raise ValueError("file-receipt-lifetime-required") from None
        self.check_link(base.captured_link)
        return _FileReceiptGuard(self.access, base)

    async def composer_files(self, files):
        files = [validate_chat_file(file) for file in files]

        async def perform():
            async with self.journal.callbacks.writer(self.access.link, self.guard()) as writer:
                result = []
                for file in files:
                    saved = await self.completed(file, session=writer)
                    result.append(dict(filename=saved.result["path"].rsplit("/", 1)[-1], size=saved.result["size"], path="/mnt/user-data/" + saved.result["path"], status="uploaded"))
                self.access._current()
                return result

        return await await_drained(perform())

    async def authorize_creation(self):
        """Retain the original Home target through a queued run's actual POST."""

        async def perform():
            async with self.journal.callbacks.writer(self.access.link, self.guard()):
                self.access._current()

        return await await_drained(perform())

    async def outcome(self, file, *, session=None):
        file = validate_chat_file(file)

        async def perform():
            link = self.access.link
            async with self.journal.callbacks.writer(link, self.guard(), session=session) as writer:
                row = await self.journal._row(writer, link, file_act_id(link, file), self.access)
                if row.continuation["file"] != file:
                    raise ValueError("file-receipt-unavailable") from None
                return _snapshot(row)

        return await await_drained(perform())

    async def completed(self, file, *, session=None):
        row = await self.outcome(file, session=session)
        if row.state != "done":
            raise ValueError("file-receipt-unavailable") from None
        return row


@dataclass(frozen=True)
class _FileReceiptGuard:
    access: object
    base: object

    @property
    def captured_link(self):
        return self.access.guard.captured_link

    def current(self):
        if self.base.current() is False:
            raise ValueError("publication-stale") from None
        self.access._current()

    async def prepare(self):
        await prepare_publication_guard(self.base)
        self.current()

    async def check(self, writer):
        await self.base.check(writer)
        current = await self.access.check_receipt(writer)
        self.current()
        return current
