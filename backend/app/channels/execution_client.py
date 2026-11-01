"""A scope-bound channel facade over a borrowed, host-owned HTTP capability.

Admission receipts and event pages do not establish final output. Optional output
reads validate the server's separately sealed source; creating POSTs never retry.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from urllib.parse import quote

from app.channels.execution import ChannelExecutionBusyError, ChannelExecutionProtocolError, ChannelExecutionScope, ChannelHttpError, ChannelHttpOwner, ChannelOwnedStream, ChannelRunAdmission, ChannelUploadRefused, JsonValue
from app.gateway.channel_execution import CHANNEL_EXECUTION_BUSY_CODE, CHANNEL_EXECUTION_REQUEST_HEADER, CHANNEL_EXECUTION_RESPONSE_HEADER, CHANNEL_TURN_ID_KEY, ChannelExecutionReceipt, ChannelExecutionRequest, receipt_from_run_kwargs
from app.gateway.run_output import OUTPUT_INVENTORY_REQUIRE_HEADER, OUTPUT_INVENTORY_VERSION_HEADER, RunOutputProjection, RunOutputProjectionV2
from deerflow.runtime.runs.output import OUTPUT_CAPTURE_KEY, capture_intent_to_dict
from deerflow.utils.file_io import await_drained


def _refuse():
    raise ChannelExecutionProtocolError() from None


def _identity(value):
    if type(value) is not str or not value or len(value) > 128 or value != value.strip() or any(not c.isprintable() or c in "/\\?#" for c in value) or value in {".", ".."}:
        _refuse()
    return value


def _date(value):
    if type(value) is not str or value != value.strip():
        _refuse()
    try:
        result = datetime.fromisoformat(value)
        if result.tzinfo is None:
            _refuse()
        return result.astimezone(UTC)
    except (ValueError, TypeError):
        _refuse()


def _cursor_time(value):
    return _date(value).isoformat().replace("+00:00", "Z")


def _snapshot(value):
    def mapping(item):
        if isinstance(item, Mapping):
            return dict(item)
        raise TypeError("Invalid channel JSON")

    return json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"), default=mapping))


@dataclass(frozen=True)
class RunCursor:
    created_at: str
    run_id: str

    def __post_init__(self):
        _date(self.created_at)
        _identity(self.run_id)

    @property
    def key(self):
        return _date(self.created_at), self.run_id


@dataclass(frozen=True)
class ValidatedRun:
    row: Mapping[str, JsonValue]
    receipt: ChannelExecutionReceipt | None


@dataclass(frozen=True)
class ValidatedRunPage:
    data: tuple[ValidatedRun, ...]
    has_more: bool
    next_before: RunCursor | None


@dataclass(frozen=True)
class ValidatedMessagePage:
    data: tuple[Mapping[str, JsonValue], ...]
    has_more: bool
    next_before_seq: int | None
    next_after_seq: int | None


class _FacadeStream:
    def __init__(self, facade, thread_id, assistant_id, kwargs):
        self._facade, self._thread, self._assistant, self._kwargs = facade, thread_id, assistant_id, kwargs
        self._inner, self._closed = None, False
        self._preparing = self._close_task = None
        self._cleaned = False
        self._next_lock = asyncio.Lock()

    def __aiter__(self):
        return self

    async def __anext__(self):
        async with self._next_lock:
            return await self._next()

    async def _next(self):
        if self._closed:
            raise StopAsyncIteration
        try:
            if self._inner is None:
                if self._preparing is None:
                    self._preparing = asyncio.create_task(self._facade._creating(self._thread, self._assistant, self._kwargs), name="channel-facade-stream-preparation")
                body, headers = await asyncio.shield(self._preparing)
                if self._closed:
                    raise StopAsyncIteration
                self._inner = self._facade._owner.stream_sse(
                    owner_user_id=self._facade.scope.owner_user_id,
                    path=self._facade._thread_path + "/runs/stream",
                    body=body,
                    headers=headers,
                    before_dispatch=lambda: self._facade._authorize("run_create"),
                    after_response=lambda: self._facade._authorize("run_read"),
                )
                self._kwargs = None
            part = await anext(self._inner)
            if self._closed:
                raise StopAsyncIteration
            self._facade._check_current("run_read")
            return part
        except ChannelHttpError as error:
            await self.aclose()
            self._facade._creating_status(error.response)
            _refuse()
        except BaseException:
            await await_drained(self.aclose())
            raise

    async def _finish_close(self):
        if self._preparing is not None:
            if not self._preparing.done():
                self._preparing.cancel()
            await asyncio.gather(self._preparing, return_exceptions=True)
        self._kwargs = None
        if self._inner is not None:
            await self._inner.aclose()
        self._preparing = None
        self._cleaned = True

    async def aclose(self):
        self._closed = True
        if self._close_task is None or self._close_task.done() and not self._cleaned:
            self._close_task = asyncio.create_task(self._finish_close(), name="channel-facade-stream-cleanup")

        async def join():
            return await asyncio.shield(self._close_task)

        await await_drained(join())


class _JoinedRunStream(_FacadeStream):
    """Read-only observation with the creating stream's retained cleanup."""

    def __init__(self, facade, run_id, after_event_id):
        super().__init__(facade, facade.scope.thread_id, facade.scope.assistant_id, {})
        self._run_id, self._after_event_id = run_id, after_event_id

    async def _next(self):
        if self._closed:
            raise StopAsyncIteration
        try:
            if self._inner is None:
                if self._preparing is None:
                    self._preparing = asyncio.create_task(self._facade._require_publication_run(self._run_id), name="channel-join-proof")
                await asyncio.shield(self._preparing)
                self._facade._check_current("run_read")
                if self._closed:
                    raise StopAsyncIteration
                self._inner = self._facade._owner.join_sse(
                    owner_user_id=self._facade.scope.owner_user_id,
                    path=self._facade._thread_path + "/runs/" + quote(self._run_id, safe="") + "/join",
                    after_event_id=self._after_event_id,
                    before_dispatch=lambda: self._facade._authorize("run_read"),
                    after_response=lambda: self._facade._authorize("run_read"),
                )
            part = await anext(self._inner)
            self._facade._check_current("run_read")
            return part
        except ChannelHttpError:
            await await_drained(self.aclose())
            _refuse()
        except BaseException:
            await await_drained(self.aclose())
            raise


class _Threads:
    def __init__(self, facade):
        self._facade = facade

    async def get(self, thread_id: str, *, headers: Mapping[str, str] | None = None) -> dict:
        f = self._facade
        if thread_id != f.scope.thread_id:
            _refuse()
        supplied = f._headers(headers, creating=False)
        supplied["X-DeerFlow-Require-Thread-Binding"] = "true"
        response = await f._request("thread_read", f._thread_path, headers=supplied)
        body = response.body
        if response.status_code != 200 or type(body) is not dict or body.get("thread_id") != thread_id or body.get("owner_user_id") != f.scope.owner_user_id or "agent_instance" not in body or body["agent_instance"] is not None:
            _refuse()
        metadata = body.get("metadata")
        if type(metadata) is not dict:
            _refuse()
        if metadata.get("channel_agent_name") != (f.scope.agent_name or f.scope.assistant_id) or metadata.get("agent_name") != f.scope.agent_name:
            _refuse()
        return body

    async def upload(self, filename: str, data: bytes) -> dict:
        """Through the route the web uses, as the bound person: it knows where this conversation's files live."""
        f = self._facade
        await f._authorize("run_create")
        try:
            response = await f._owner.request_json(
                operation="run_create",
                owner_user_id=f.scope.owner_user_id,
                method="POST",
                path=f._thread_path + "/uploads",
                # With no type, httpx guesses one, and the first guess in a process reads files on this loop.
                files=[("files", (filename, data, "application/octet-stream"))],
                before_dispatch=lambda: f._authorize("run_create"),
                after_response=lambda: f._authorize("run_create"),
            )
        except ChannelExecutionProtocolError as error:
            error.__traceback__ = None  # Its frames hold the file's bytes, which the next file's budget needs freed.
            # Only a turn that ended takes the message with it. Anything else costs it this file.
            f._check_current("run_create")
            raise ChannelUploadRefused(None) from None
        f._check_current("run_create")
        body = response.body
        if response.status_code != 200 or type(body) is not dict or body.get("success") is not True or type(body.get("files")) is not list or len(body["files"]) != 1:
            raise ChannelUploadRefused(response.status_code)
        return body["files"][0]


class _Runs:
    def __init__(self, facade):
        self._facade = facade

    async def wait(
        self,
        thread_id: str,
        assistant_id: str,
        *,
        input: dict,
        config: dict | None = None,
        context: dict | None = None,
        metadata: dict | None = None,
        multitask_strategy="reject",
        channel_execution: Mapping[str, JsonValue],
        on_disconnect="continue",
        on_completion=None,
        headers: Mapping[str, str] | None = None,
    ) -> JsonValue:
        f = self._facade
        body, headers = await f._creating(
            thread_id,
            assistant_id,
            dict(input=input, config=config, context=context, metadata=metadata, multitask_strategy=multitask_strategy, channel_execution=channel_execution, on_disconnect=on_disconnect, on_completion=on_completion, headers=headers),
        )
        response = await f._owner.request_json(
            operation="run_create",
            owner_user_id=f.scope.owner_user_id,
            method="POST",
            path=f._thread_path + "/runs/wait",
            body=body,
            headers=headers,
            before_dispatch=lambda: f._authorize("run_create"),
            after_response=lambda: f._authorize("run_read"),
        )
        f._creating_status(response)
        f._check_current("run_read")
        return response.body

    def stream(
        self,
        thread_id: str,
        assistant_id: str,
        *,
        input: dict,
        config: dict | None = None,
        context: dict | None = None,
        metadata: dict | None = None,
        stream_mode: str | Sequence[str] | None = None,
        multitask_strategy="reject",
        channel_execution: Mapping[str, JsonValue],
        on_disconnect="continue",
        on_completion=None,
        headers: Mapping[str, str] | None = None,
    ) -> ChannelOwnedStream:
        return _FacadeStream(
            self._facade,
            thread_id,
            assistant_id,
            dict(
                input=input,
                config=config,
                context=context,
                metadata=metadata,
                stream_mode=stream_mode,
                multitask_strategy=multitask_strategy,
                channel_execution=channel_execution,
                on_disconnect=on_disconnect,
                on_completion=on_completion,
                headers=headers,
            ),
        )


class ChannelExecutionFacade:
    def __init__(self, owner: ChannelHttpOwner, *, scope: ChannelExecutionScope, admission: ChannelRunAdmission, capture_output: bool = False, capture_inventory: bool = False, inventory_document_version: int = 1):
        if (
            type(capture_output) is not bool
            or type(capture_inventory) is not bool
            or type(inventory_document_version) is not int
            or inventory_document_version not in (1, 2)
            or inventory_document_version == 2
            and not capture_inventory
            or capture_inventory
            and not capture_output
            or type(scope) is not ChannelExecutionScope
            or scope.assistant_id != "lead_agent"
            or owner.endpoint_id != scope.endpoint_id
            or not callable(getattr(admission, "authorize", None))
            or not callable(getattr(admission, "current", None))
        ):
            _refuse()
        _identity(scope.thread_id)
        self._owner, self._scope, self._admission, self._capture_output, self._capture_inventory = owner, scope, admission, capture_output, capture_inventory
        self._inventory_document_version = inventory_document_version
        self._thread_path = "threads/" + quote(scope.thread_id, safe="")
        self.threads, self.runs = _Threads(self), _Runs(self)

    @property
    def scope(self):
        return self._scope

    @property
    def capture_output(self) -> bool:
        return self._capture_output

    @property
    def capture_inventory(self) -> bool:
        return self._capture_inventory

    @property
    def inventory_document_version(self) -> int:
        return self._inventory_document_version

    async def _authorize(self, operation):
        try:
            if self._owner.endpoint_id != self.scope.endpoint_id:
                _refuse()
            await await_drained(self._admission.authorize(operation))
            self._check_current(operation)
        except Exception:
            _refuse()

    def _check_current(self, operation):
        try:
            if self._owner.endpoint_id != self.scope.endpoint_id or self._admission.current(operation) is not True:
                _refuse()
        except Exception:
            _refuse()

    def _headers(self, headers, *, creating):
        if headers is not None and not isinstance(headers, Mapping):
            _refuse()
        result = dict(headers or {})
        allowed = {"idempotency-key"} if creating else {"x-deerflow-require-thread-binding", CHANNEL_EXECUTION_REQUEST_HEADER.lower()}
        if any(type(k) is not str or k.lower() not in allowed or type(v) is not str for k, v in result.items()) or len({k.lower() for k in result}) != len(result):
            _refuse()
        if creating and {key.lower(): value for key, value in result.items()} != {"idempotency-key": self.scope.publication_id}:
            _refuse()
        return result

    async def _creating(self, thread_id, assistant_id, kwargs):
        if type(thread_id) is not str or type(assistant_id) is not str or thread_id != self.scope.thread_id or assistant_id != self.scope.assistant_id:
            _refuse()
        try:
            private = await await_drained(asyncio.to_thread(_snapshot, kwargs))
            headers = self._headers(private.pop("headers"), creating=True)
            ChannelExecutionRequest.model_validate(private["channel_execution"])
            if (
                private["channel_execution"] != {"version": 1, "publication_id": self.scope.publication_id}
                or type(private["input"]) is not dict
                or not private["input"]
                or private["multitask_strategy"] != "reject"
                or private["on_disconnect"] != "continue"
                or private["on_completion"] is not None
            ):
                _refuse()
            for carrier in (private.get("config"), private.get("context"), private.get("metadata")):
                if carrier is not None and type(carrier) is not dict:
                    _refuse()

            def inspect(carrier):
                if carrier is None:
                    return
                if type(carrier) is not dict:
                    _refuse()
                for key, expected in (
                    ("agent_name", self.scope.agent_name),
                    ("assistant_id", self.scope.assistant_id),
                    ("thread_id", self.scope.thread_id),
                    ("user_id", self.scope.owner_user_id),
                    ("owner_user_id", self.scope.owner_user_id),
                    ("connection_id", self.scope.connection_id),
                    ("chat_id", self.scope.chat_id),
                    ("endpoint_id", self.scope.endpoint_id),
                    ("channel_agent_name", self.scope.agent_name or self.scope.assistant_id),
                    (CHANNEL_TURN_ID_KEY, self.scope.publication_id),
                ):
                    if key in carrier and carrier[key] != expected:
                        _refuse()
                if any(key in carrier and carrier[key] is not None for key in ("agent_instance", "agent_instance_id")):
                    _refuse()
                if "is_bootstrap" in carrier and carrier["is_bootstrap"] is not None and carrier["is_bootstrap"] is not False:
                    _refuse()
                for key in ("config", "configurable", "context", "metadata", "human_message_kwargs"):
                    if key in carrier:
                        inspect(carrier[key])

            for carrier in (private.get("config"), private.get("context"), private.get("metadata")):
                inspect(carrier)
            if "stream_mode" in private and private["stream_mode"] is not None and not (type(private["stream_mode"]) is str or type(private["stream_mode"]) is list and all(type(value) is str for value in private["stream_mode"])):
                _refuse()
            private["assistant_id"] = self.scope.assistant_id
            private["context"] = (private.get("context") or {}) | {"agent_name": self.scope.agent_name}
            if self.capture_output:
                private[OUTPUT_CAPTURE_KEY] = capture_intent_to_dict({"version": 2 if self.capture_inventory else 1, **({"inventory_document_version": 2} if self.inventory_document_version == 2 else {})})
            return private, headers
        except Exception:
            _refuse()

    @staticmethod
    def _creating_status(response):
        if response.status_code == 409 and response.body == {"detail": {"code": CHANNEL_EXECUTION_BUSY_CODE}}:
            raise ChannelExecutionBusyError()
        if not 200 <= response.status_code < 300:
            _refuse()

    async def _request(self, operation, path, *, params=None, headers=None):
        response = await self._owner.request_json(
            operation=operation, owner_user_id=self.scope.owner_user_id, method="GET", path=path, params=params, headers=headers, before_dispatch=lambda: self._authorize(operation), after_response=lambda: self._authorize(operation)
        )
        self._check_current(operation)
        return response

    async def _proof(self, operation, path, *, params=None, missing=False):
        response = await self._request(operation, path, params=params, headers={CHANNEL_EXECUTION_REQUEST_HEADER: "1"})
        if response.headers.get(CHANNEL_EXECUTION_RESPONSE_HEADER.lower()) != "1":
            _refuse()
        if missing and response.status_code == 404:
            return None
        if response.status_code != 200:
            _refuse()
        if response.body is None:
            _refuse()
        return response.body

    def _run(self, row, *, run_id=None):
        try:
            if type(row) is not dict or row.get("thread_id") != self.scope.thread_id or run_id is not None and row.get("run_id") != run_id:
                _refuse()
            _identity(row.get("run_id"))
            _date(row.get("created_at"))
            if row.get("status") not in {"pending", "running", "success", "error", "timeout", "interrupted"}:
                _refuse()
            if any(key in row and row[key] != self.scope.owner_user_id for key in ("owner_user_id", "user_id")):
                _refuse()
            receipt = receipt_from_run_kwargs(row.get("kwargs"))
            if receipt is not None:
                if receipt.thread_id != self.scope.thread_id or receipt.owner_user_id != self.scope.owner_user_id:
                    _refuse()
                alias = row.get("assistant_id")
                if alias not in (None, receipt.assistant_id) and (receipt.agent_name is None or type(alias) is not str or alias.strip().lower().replace("_", "-") != receipt.agent_name):
                    _refuse()
            return ValidatedRun(MappingProxyType(row), receipt)
        except Exception:
            _refuse()

    async def get_run(self, run_id: str) -> ValidatedRun | None:
        run_id = _identity(run_id)
        result = await self._proof("run_read", self._thread_path + "/runs/" + quote(run_id, safe=""), missing=True)
        return None if result is None else self._run(result, run_id=run_id)

    async def _require_publication_run(self, run_id):
        run = await self.get_run(run_id)
        return self._publication_run(run)

    def _publication_run(self, run):
        proof = run.receipt if run is not None else None
        if (
            proof is None
            or (proof.owner_user_id, proof.thread_id, proof.publication_id, proof.assistant_id, proof.agent_name) != (self.scope.owner_user_id, self.scope.thread_id, self.scope.publication_id, self.scope.assistant_id, self.scope.agent_name)
            or type(run.row.get("metadata")) is not dict
            or run.row["metadata"].get(CHANNEL_TURN_ID_KEY) != self.scope.publication_id
        ):
            _refuse()
        return run

    async def lookup_run(self) -> ValidatedRun | None:
        """Read this exact POST key; absence grants no creating fallback."""
        response = await self._request("run_read", self._thread_path + "/runs/by-idempotency-key", headers={CHANNEL_EXECUTION_REQUEST_HEADER: "1", "Idempotency-Key": self.scope.publication_id})
        if response.headers.get(CHANNEL_EXECUTION_RESPONSE_HEADER.lower()) != "1":
            _refuse()
        if response.status_code == 404:
            if type(response.body) is not dict or response.body.get("lookup") != "idempotency-key":
                _refuse()
            return None
        if response.status_code != 200 or response.body is None:
            _refuse()
        run = self._publication_run(self._run(response.body))
        self._check_current("run_read")
        return run

    async def cancel_run(self, run_id: str) -> str:
        run_id = _identity(run_id)
        await self._authorize("run_cancel")
        run = await self._require_publication_run(run_id)
        if run.row["status"] == "interrupted" and run.row.get("stop_reason") is None:
            return "requested"  # Already cancelled, and only the answer to that request was lost.
        if run.row["status"] in {"success", "error", "timeout", "interrupted"}:
            return "terminal"
        response = await self._owner.request_json(
            operation="run_cancel",
            owner_user_id=self.scope.owner_user_id,
            method="POST",
            path=self._thread_path + "/runs/" + quote(run_id, safe="") + "/cancel",
            params={"wait": False, "action": "interrupt"},
            before_dispatch=lambda: self._authorize("run_cancel"),
            after_response=lambda: self._authorize("run_cancel"),
        )
        self._check_current("run_cancel")
        if response.status_code == 409:
            return "refused"
        if response.status_code not in {202, 204}:
            _refuse()
        return "requested"

    def join_run(self, run_id: str, *, after_event_id: str | None = None) -> ChannelOwnedStream:
        run_id = _identity(run_id)
        if after_event_id is not None and (type(after_event_id) is not str or not after_event_id or len(after_event_id) > 256 or not after_event_id.isprintable()):
            _refuse()
        return _JoinedRunStream(self, run_id, after_event_id)

    async def get_run_output(self, run_id: str) -> RunOutputProjection | RunOutputProjectionV2 | None:
        run_id = _identity(run_id)
        path = self._thread_path + "/runs/" + quote(run_id, safe="") + "/output"
        if self.capture_inventory:
            version = str(self.inventory_document_version)
            response = await self._request("source_read", path, headers={CHANNEL_EXECUTION_REQUEST_HEADER: "1", OUTPUT_INVENTORY_REQUIRE_HEADER: version})
            if response.headers.get(CHANNEL_EXECUTION_RESPONSE_HEADER.lower()) != "1" or response.headers.get(OUTPUT_INVENTORY_VERSION_HEADER.lower()) != version:
                _refuse()
            if response.status_code == 404:
                return None
            if response.status_code != 200 or response.body is None:
                _refuse()
            result = response.body
        else:
            result = await self._proof("source_read", path, missing=True)
        if result is None:
            return None
        try:
            projection = RunOutputProjectionV2.model_validate(result) if self.capture_inventory else RunOutputProjection.model_validate(result)
            if self.capture_inventory and projection.inventory.document is not None and projection.inventory.document.version > self.inventory_document_version:
                _refuse()
            output = projection.output if self.capture_inventory else projection
            channel = output.channel_execution
            if (output.owner_user_id, output.thread_id, output.run_id) != (self.scope.owner_user_id, self.scope.thread_id, run_id) or (channel.publication_id, channel.assistant_id, channel.agent_name) != (
                self.scope.publication_id,
                self.scope.assistant_id,
                self.scope.agent_name,
            ):
                _refuse()
            self._check_current("source_read")
            return projection
        except Exception:
            _refuse()

    async def list_runs_page(self, *, limit: int = 100, before: RunCursor | None = None) -> ValidatedRunPage:
        if type(limit) is not int or not 1 <= limit <= 200 or before is not None and type(before) is not RunCursor:
            _refuse()
        params = {"limit": limit}
        if before is not None:
            params.update(before_created_at=before.created_at, before_run_id=before.run_id)
        result = await self._proof("run_read", self._thread_path + "/runs/page", params=params)
        if type(result) is not dict or type(result.get("data")) is not list or type(result.get("has_more")) is not bool or len(result["data"]) > limit:
            _refuse()
        data = tuple(self._run(row) for row in result["data"])
        keys = [(_date(run.row["created_at"]), run.row["run_id"]) for run in data]
        if len({run.row["run_id"] for run in data}) != len(data) or any(first <= second for first, second in zip(keys, keys[1:])) or before is not None and any(key >= before.key for key in keys):
            _refuse()
        more = result["has_more"]
        continuation = result.get("next_before_created_at"), result.get("next_before_run_id")
        next_before = None
        if more:
            if not data or continuation != (_cursor_time(data[-1].row["created_at"]), data[-1].row["run_id"]):
                _refuse()
            next_before = RunCursor(*continuation)
        elif continuation != (None, None):
            _refuse()
        return ValidatedRunPage(data, more, next_before)

    async def list_run_messages(self, run_id: str, *, limit: int = 100, before_seq: int | None = None, after_seq: int | None = None) -> ValidatedMessagePage:
        run_id = _identity(run_id)
        if type(limit) is not int or not 1 <= limit <= 200 or any(value is not None and (type(value) is not int or value <= 0) for value in (before_seq, after_seq)):
            _refuse()
        params = {"limit": limit}
        if before_seq is not None:
            params["before_seq"] = before_seq
        if after_seq is not None:
            params["after_seq"] = after_seq
        result = await self._proof("source_read", self._thread_path + "/runs/" + quote(run_id, safe="") + "/messages", params=params)
        if type(result) is not dict or type(result.get("data")) is not list or type(result.get("has_more")) is not bool or len(result["data"]) > limit:
            _refuse()
        data = result["data"]
        previous = 0
        for row in data:
            if (
                type(row) is not dict
                or row.get("thread_id") != self.scope.thread_id
                or row.get("run_id") != run_id
                or type(row.get("seq")) is not int
                or row["seq"] <= previous
                or row.get("category") != "message"
                or type(row.get("event_type")) is not str
                or not row["event_type"]
                or type(row.get("content")) not in {str, dict}
                or type(row.get("metadata")) is not dict
            ):
                _refuse()
            _date(row.get("created_at"))
            previous = row["seq"]
            if before_seq is not None and previous >= before_seq or after_seq is not None and previous <= after_seq:
                _refuse()
        if result["has_more"] and not data:
            _refuse()
        return ValidatedMessagePage(tuple(MappingProxyType(row) for row in data), result["has_more"], data[0]["seq"] if data and result["has_more"] else None, data[-1]["seq"] if data and result["has_more"] else None)

    async def find_publication_run(self) -> ValidatedRun | None:
        before, match, seen = None, None, set()
        while True:
            page = await self.list_runs_page(before=before)
            for run in page.data:
                identity = run.row["run_id"]
                if identity in seen:
                    _refuse()
                seen.add(identity)
                proof = run.receipt
                if proof is not None and proof.publication_id == self.scope.publication_id and proof.assistant_id == self.scope.assistant_id and proof.agent_name == self.scope.agent_name:
                    if match is not None:
                        _refuse()
                    match = run
            if not page.has_more:
                return match
            if page.next_before is None or before is not None and page.next_before.key >= before.key:
                _refuse()
            before = page.next_before
