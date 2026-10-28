"""One loop-owned HTTP/SDK client for preparation and host channel capabilities."""

from __future__ import annotations

import asyncio
import io
import json
import re
from collections.abc import Mapping
from contextvars import ContextVar
from dataclasses import dataclass
from hashlib import sha256
from typing import TYPE_CHECKING, Literal
from urllib.parse import quote, unquote, urlsplit

import httpx

from app.channels.execution import BeforeDispatch, ChannelExecutionProtocolError, ChannelHttpError, ChannelJsonResponse, ChannelOperation, ChannelOwnedStream, JsonValue, QueryValue
from deerflow.utils.file_io import await_drained
from deerflow.utils.thread_id import validate_thread_id

if TYPE_CHECKING:
    from ssl import SSLContext

_BEFORE_DISPATCH = ContextVar("telegram_thread_before_dispatch", default=None)


@dataclass(frozen=True)
class ArtifactMetadata:
    """Current source consistency data; never a grant to read the file."""

    path: str
    filename: str
    size: int
    mime_type: str


def _artifact_reference(thread_id, path):
    try:
        validate_thread_id(thread_id)
        if type(path) is not str or not path.startswith("/mnt/user-data/outputs/") or len(path) > 4096 or any(ord(char) < 32 or ord(char) == 127 or char == "\\" for char in path):
            raise ValueError
        parts = path[1:].split("/")
        if any(not part or part in {".", ".."} for part in parts) or ".skill/" in path:
            raise ValueError
        return path, "/api/threads/" + quote(thread_id, safe="") + "/artifacts/" + "/".join(quote(part, safe="") for part in parts)
    except (TypeError, ValueError):
        raise ChannelExecutionProtocolError() from None


def _artifact_length(response):
    value = response.headers.get("content-length")
    if value is None:
        return None
    if len(value) > 19 or not value.isascii() or not value.isdecimal() or int(value) > 2**63 - 1:
        raise ChannelExecutionProtocolError()
    return int(value)


def _discard_error_frames(error):
    """Converted transport failures cannot retain charged byte aliases."""
    pending, seen = [error], set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        pending.extend(linked for linked in (current.__cause__, current.__context__) if linked is not None)
        current.__traceback__ = None


async def _before_request(request) -> None:
    check = _BEFORE_DISPATCH.get()
    if check is not None:
        await check()


def _endpoint(value: str) -> str:
    try:
        if not isinstance(value, str) or any(ord(char) <= 32 or char == "\\" for char in value):
            raise ValueError
        url = httpx.URL(value)
        if url.scheme not in {"http", "https"} or not url.host or url.userinfo or url.query or url.fragment or "?" in value or "#" in value:
            raise ValueError
        return str(url).rstrip("/")
    except (ValueError, httpx.InvalidURL):
        raise ValueError("invalid-thread-endpoint") from None


def _construct(base_url, tls_context, transport):
    # SDK/CSRF imports and HTTPX/TLS construction may read files synchronously.
    # The caller retains this worker's result until loop-side close.
    from langgraph_sdk.client import LangGraphClient
    from langgraph_sdk.sse import SSEDecoder, aiter_lines_raw

    from app.gateway.agent_discovery import THREAD_BINDING_HEADER_NAME
    from app.gateway.csrf_middleware import CSRF_COOKIE_NAME, CSRF_HEADER_NAME, generate_csrf_token
    from app.gateway.internal_auth import create_internal_auth_headers

    csrf = generate_csrf_token()
    headers = {**create_internal_auth_headers(), CSRF_HEADER_NAME: csrf, "Cookie": f"{CSRF_COOKIE_NAME}={csrf}"}
    client = httpx.AsyncClient(base_url=base_url + "/", verify=tls_context if tls_context is not None else True, trust_env=False, timeout=httpx.Timeout(30), transport=transport, event_hooks={"request": [_before_request]})
    return LangGraphClient(client), headers, THREAD_BINDING_HEADER_NAME, client, (SSEDecoder, aiter_lines_raw)


def _encode(value):
    def mapping(item):
        if isinstance(item, Mapping):
            return dict(item)
        raise TypeError("Invalid channel JSON")

    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"), default=mapping).encode("utf-8")


def _decode(value):
    def invalid_constant(_value):
        raise ValueError("Invalid channel JSON")

    return json.loads(value, parse_constant=invalid_constant) if value else None


def _json_response(status, headers, value):
    return ChannelJsonResponse(status, headers, _decode(value))


def _authority_headers(owner):
    from app.gateway.csrf_middleware import CSRF_COOKIE_NAME, CSRF_HEADER_NAME, generate_csrf_token
    from app.gateway.internal_auth import create_internal_auth_headers

    csrf = generate_csrf_token()
    return {**create_internal_auth_headers(owner_user_id=owner), CSRF_HEADER_NAME: csrf, "Cookie": f"{CSRF_COOKIE_NAME}={csrf}"}


def _path(value):
    if type(value) is not str or not value or value.startswith("/") or any(ord(c) <= 32 or ord(c) == 127 or c in "\\?#" for c in value):
        raise ChannelExecutionProtocolError()
    parsed = urlsplit(value)
    if (
        parsed.scheme
        or parsed.netloc
        or any(not part or unquote(part) in {".", ".."} or any(c in unquote(part) for c in "/\\") or any(ord(c) <= 32 or ord(c) == 127 for c in unquote(part)) or re.search(r"%(?![0-9a-fA-F]{2})", part) for part in value.split("/"))
    ):
        raise ChannelExecutionProtocolError()
    return value


def _extra_headers(headers, *, streaming=False, replay=False):
    result = {}
    for key, value in (headers or {}).items():
        if type(key) is not str or type(value) is not str or any(ord(c) <= 31 or ord(c) == 127 for c in value):
            raise ChannelExecutionProtocolError()
        name = key.lower()
        if name in result:
            raise ChannelExecutionProtocolError()
        if name == "idempotency-key":
            if not value or len(value) > 256 or any(c.isspace() for c in value):
                raise ChannelExecutionProtocolError()
        elif name == "x-deerflow-require-thread-binding":
            if value != "true":
                raise ChannelExecutionProtocolError()
        elif name == "x-deerflow-require-output-inventory":
            if value not in {"1", "2"}:
                raise ChannelExecutionProtocolError()
        elif name == "x-deerflow-require-channel-execution":
            if value != "1":
                raise ChannelExecutionProtocolError()
        elif name == "last-event-id":
            if not replay or not value or len(value) > 256 or not value.isprintable():
                raise ChannelExecutionProtocolError()
        elif name == "accept":
            if value != ("text/event-stream" if streaming else "application/json"):
                raise ChannelExecutionProtocolError()
        elif name == "content-type":
            if value != "application/json":
                raise ChannelExecutionProtocolError()
        else:
            raise ChannelExecutionProtocolError()
        result[name] = value
    return result


def _response_headers(response):
    return {key: value for key, value in response.headers.items() if key in {"content-type", "x-deerflow-channel-execution-version", "x-deerflow-output-inventory-version"}}


class _OwnedStream:
    def __init__(self, owner, kwargs):
        self._owner, self._kwargs = owner, kwargs
        self._queue = asyncio.Queue(maxsize=1)
        self._pump_task = self._close_task = None
        self._closed = False
        self._response = None
        self._cleanup_failed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._closed:
            raise StopAsyncIteration
        try:
            if self._pump_task is None:
                self._owner._check_operation(self._kwargs.get("operation", "run_create"))
                self._owner._streams.add(self)
                self._pump_task = self._owner._register(self._pump(), "channel-http-stream")
            kind, value = await self._queue.get()
            if self._owner._stopping:
                raise ChannelExecutionProtocolError()
            if kind == "part":
                return value
            await self.aclose()
            if kind == "error":
                raise value from None
            raise StopAsyncIteration
        except asyncio.CancelledError:
            await await_drained(self.aclose())
            if self._owner._stopping:
                await await_drained(self._owner.stop())
            raise

    async def _pump(self):
        try:
            owner, kw = self._owner, self._kwargs
            operation = kw.get("operation", "run_create")
            await owner.start()
            body = await await_drained(asyncio.to_thread(_encode, kw["body"])) if kw.get("body") is not None else None
            headers = await owner._headers_for(kw["owner_user_id"], kw["headers"], streaming=True, replay=operation == "run_read")
            owner._check_operation(operation)
            context = _BEFORE_DISPATCH.set(owner._dispatch_check(operation, kw["before_dispatch"]))
            try:
                response = await owner._http_client.send(
                    owner._http_client.build_request(
                        "POST" if operation == "run_create" else "GET", kw["path"], content=body, headers=headers, timeout=httpx.Timeout(connect=5, pool=5, read=300, write=300) if operation == "run_create" else httpx.Timeout(30)
                    ),
                    stream=True,
                )
                self._response = response
                owner._remember_response(response)
                try:
                    if not 200 <= response.status_code < 300:
                        data = await owner._response_content(response)
                        result = await await_drained(asyncio.to_thread(_json_response, response.status_code, _response_headers(response), data))
                        await owner._post_response(operation, kw["after_response"])
                        raise await await_drained(asyncio.to_thread(ChannelHttpError, result))
                    if response.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "text/event-stream":
                        raise ChannelExecutionProtocolError()
                    decoder_factory, lines = owner._sse
                    decoder = decoder_factory()
                    async for line in lines(response):
                        part = await await_drained(asyncio.to_thread(decoder.decode, line))
                        if part is not None:
                            await owner._post_response(operation, kw["after_response"])
                            await self._queue.put(("part", part))
                    if response.is_closed:
                        owner._responses[response]["confirmed"] = True
                finally:
                    await owner._close_response(response)
            finally:
                _BEFORE_DISPATCH.reset(context)
        except asyncio.CancelledError:
            raise
        except ChannelHttpError as error:
            await self._queue.put(("error", error))
        except Exception:
            await self._queue.put(("error", ChannelExecutionProtocolError()))
        else:
            await self._queue.put(("end", None))

    async def _finish_close(self):
        if self._pump_task is not None:
            if not self._pump_task.done():
                self._pump_task.cancel()
            await asyncio.gather(self._pump_task, return_exceptions=True)
        if self._response in self._owner._responses:
            if not self._cleanup_failed:
                self._cleanup_failed = True
                raise ChannelExecutionProtocolError()
            await self._owner._close_response(self._response)
        self._closed = True
        self._kwargs = None
        while not self._queue.empty():
            self._queue.get_nowait()
        self._queue.put_nowait(("end", None))
        self._owner._streams.discard(self)

    async def aclose(self):
        if self._close_task is None or self._close_task.done() and not self._closed:
            self._close_task = asyncio.create_task(self._finish_close(), name="channel-stream-cleanup")
        await await_drained(GatewayClientOwner._join(self._close_task))


class GatewayClientOwner:
    def __init__(self, *, gateway_url: str, langgraph_url: str, tls_context: SSLContext | None = None, transport=None):
        self._gateway_url = gateway_url.rstrip("/")
        self._base_url = _endpoint(langgraph_url)
        self._endpoint_id = sha256(self._base_url.encode("utf-8")).hexdigest()
        self._tls_context, self._transport = tls_context, transport
        self._client, self._headers = None, None
        self._http_client = None
        self._sse = None
        self._binding_header = None
        self._construction: asyncio.Task | None = None
        self._stop_task: asyncio.Task | None = None
        self._requests: set[asyncio.Task] = set()
        self._operations: set[asyncio.Task] = set()
        self._streams: set[_OwnedStream] = set()
        self._responses: dict[httpx.Response, dict] = {}
        self._run_admission_closed = False
        self._stopping, self._closed = False, False

    @property
    def gateway_url(self) -> str:
        return self._gateway_url

    @property
    def endpoint_id(self) -> str:
        return self._endpoint_id

    @property
    def closed(self) -> bool:
        return self._closed

    async def _start_owned(self) -> None:
        self._client, self._headers, self._binding_header, self._http_client, self._sse = await asyncio.to_thread(_construct, self._base_url, self._tls_context, self._transport)

    async def start(self) -> None:
        if self._stopping:
            raise RuntimeError("thread-client-stopping")
        if self._client is not None:
            return
        if self._construction is None:
            self._construction = asyncio.create_task(self._start_owned(), name="telegram-thread-client-construction")
        try:
            await await_drained(self._join(self._construction))
        except asyncio.CancelledError:
            self.begin_shutdown()
            if asyncio.current_task() in self._operations:
                # The caller first retires this tracked operation, then joins
                # final cleanup; cleanup itself must not wait on this waiter.
                raise
            await await_drained(self.stop())
            raise
        if self._stopping:
            raise RuntimeError("thread-client-stopping")

    @staticmethod
    async def _join(task):
        return await asyncio.shield(task)

    def owner_headers(self, owner_user_id: str) -> dict[str, str]:
        if not isinstance(owner_user_id, str) or not owner_user_id or any(ord(char) <= 32 or ord(char) == 127 for char in owner_user_id):
            raise ValueError("invalid-thread-owner")
        if self._headers is None:
            raise RuntimeError("thread-client-not-started")
        return {**self._headers, "X-DeerFlow-Owner-User-Id": owner_user_id}

    async def _request(self, operation, before_dispatch):
        await self.start()
        if self._stopping:
            raise RuntimeError("thread-client-stopping")

        async def run():
            context = _BEFORE_DISPATCH.set(before_dispatch)
            try:
                return await operation(self._client)
            finally:
                _BEFORE_DISPATCH.reset(context)

        task = asyncio.create_task(run(), name="telegram-thread-http")
        self._requests.add(task)
        task.add_done_callback(self._requests.discard)
        # SDK JSON decoding owns an executor worker. Do not cancel the admitted
        # operation or let its waiter release the client/lock before it finishes.
        return await await_drained(self._join(task))

    async def get_thread(self, owner_user_id: str, thread_id: str, *, before_dispatch=None) -> dict:
        async def get(client):
            return await client.threads.get(thread_id, headers={**self.owner_headers(owner_user_id), self._binding_header: "true"})

        return await self._request(get, before_dispatch)

    async def create_thread(self, owner_user_id: str, *, thread_id: str, metadata: dict, before_dispatch=None) -> dict:
        async def create(client):
            return await client.threads.create(thread_id=thread_id, metadata=metadata, headers=self.owner_headers(owner_user_id))

        return await self._request(create, before_dispatch)

    async def get_features(self, owner_user_id: str, *, before_dispatch) -> dict:
        """Read storage availability from the separately configured Gateway.

        Borrow this owner's HTTP client and cleanup, with fresh bound-human
        headers. The fixed URL never comes from a Telegram file descriptor.
        """

        async def read(_client):
            self._check_operation("thread_read")
            headers = await self._headers_for(owner_user_id, None)
            await before_dispatch()
            self._check_operation("thread_read")
            response = await self._http_client.send(self._http_client.build_request("GET", self._gateway_url + "/api/features", headers=headers, timeout=httpx.Timeout(30)), stream=True, follow_redirects=False)
            self._remember_response(response)
            try:
                content = await self._response_content(response)
                if response.status_code != 200:
                    raise ChannelExecutionProtocolError()
                result = await await_drained(asyncio.to_thread(_decode, content))
                if type(result) is not dict:
                    raise ChannelExecutionProtocolError()
                await before_dispatch()
                self._check_operation("thread_read")
                return result
            finally:
                await self._close_response(response)

        try:
            return await self._request(read, None)
        except asyncio.CancelledError:
            raise
        except Exception:
            raise ChannelExecutionProtocolError() from None

    async def describe_artifact(self, owner_user_id: str, thread_id: str, path: str, *, before_dispatch) -> ArtifactMetadata:
        """Read only current authorized metadata, before allocating file bytes."""
        return await self._artifact_request(owner_user_id, thread_id, path, before_dispatch=before_dispatch)

    async def read_artifact_capped(self, owner_user_id: str, thread_id: str, path: str, *, ceiling: int, before_dispatch, on_failed_close=None) -> bytes:
        """Read raw bytes through the same owned client after batch admission."""
        if type(ceiling) is not int or not 0 <= ceiling <= 2**63 - 1 or on_failed_close is not None and not callable(on_failed_close):
            raise ChannelExecutionProtocolError()
        return await self._artifact_request(owner_user_id, thread_id, path, before_dispatch=before_dispatch, ceiling=ceiling, on_failed_close=on_failed_close)

    def retains_failed_close(self, callback) -> bool:
        return callback is not None and any(state["close_failed"] and state["on_failed_close"] is callback for state in self._responses.values())

    async def _artifact_request(self, owner_user_id, thread_id, path, *, before_dispatch, ceiling=None, on_failed_close=None):
        path, relative = _artifact_reference(thread_id, path)
        if not callable(before_dispatch):
            raise ChannelExecutionProtocolError()
        url = _endpoint(self._gateway_url) + relative
        handoff = []

        async def admit():
            await await_drained(before_dispatch())
            self._check_operation("source_read")

        async def run():
            buffer = io.BytesIO()
            response = request = chunk = None
            try:
                await admit()
                await self.start()
                headers = await self._headers_for(owner_user_id, None)
                headers.update({"Accept": "application/octet-stream", "Accept-Encoding": "identity"})
                headers.pop("Content-Type", None)
                await admit()
                context = _BEFORE_DISPATCH.set(self._dispatch_check("source_read", before_dispatch))
                try:
                    request = self._http_client.build_request("HEAD" if ceiling is None else "GET", url, params={"download": "true", **({"stream": "true"} if ceiling is not None else {})}, headers=headers, timeout=httpx.Timeout(30))
                    response = await self._http_client.send(request, stream=True, follow_redirects=False)
                except (httpx.HTTPError, OSError) as error:
                    _discard_error_frames(error)
                    raise ChannelExecutionProtocolError() from None
                finally:
                    _BEFORE_DISPATCH.reset(context)
                self._remember_response(response, on_failed_close=on_failed_close)
                try:
                    await admit()
                    if response.status_code != 200:
                        raise ChannelHttpError(ChannelJsonResponse(response.status_code, _response_headers(response), None))
                    if response.headers.get("content-encoding", "identity").strip().lower() != "identity":
                        raise ChannelExecutionProtocolError()
                    length = _artifact_length(response)
                    if ceiling is None:
                        if length is None:
                            raise ChannelExecutionProtocolError()
                        mime = response.headers.get("content-type", "application/octet-stream").split(";", 1)[0].strip().lower()
                        if len(mime) > 255 or re.fullmatch(r"[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+", mime) is None:
                            raise ChannelExecutionProtocolError()
                        result = ArtifactMetadata(path, path.rsplit("/", 1)[1], length, mime)
                    else:
                        if length is not None and length > ceiling:
                            raise ChannelExecutionProtocolError()
                        size = 0
                        failed = False
                        try:
                            # Public raw stream avoids decompression, a whole
                            # HTTPX body cache and implicit aiter_raw close.
                            async for chunk in response.stream:
                                self._check_operation("source_read")
                                size += len(chunk)
                                if size > ceiling:
                                    raise ChannelExecutionProtocolError()
                                buffer.write(chunk)
                        except (asyncio.CancelledError, ChannelExecutionProtocolError):
                            raise
                        except Exception as error:
                            _discard_error_frames(error)
                            failed = True
                        if failed or length is not None and length != size:
                            raise ChannelExecutionProtocolError()
                finally:
                    await self._close_response(response)
                await admit()
                # The retained task returns no payload: failed/cancelled public
                # handoff can clear this cell even if its traceback is retained.
                handoff.append(result if ceiling is None else buffer.getvalue())
            finally:
                response = request = chunk = None
                buffer.close()

        self._check_operation("source_read")
        task = self._register(run(), "channel-artifact-read")
        try:
            await asyncio.shield(task)
            await admit()
            return handoff.pop()
        except asyncio.CancelledError:
            task.cancel()
            await await_drained(self._join_retired(task))
            if self._stopping:
                await await_drained(self.stop())
            raise
        finally:
            handoff.clear()

    def _check_operation(self, operation):
        if operation not in {"thread_read", "run_read", "source_read", "run_create", "run_cancel"} or self._stopping or operation == "run_create" and self._run_admission_closed:
            raise ChannelExecutionProtocolError()

    def _register(self, coro, name):
        task = asyncio.create_task(coro, name=name)
        self._operations.add(task)
        task.add_done_callback(self._operations.discard)
        return task

    def _dispatch_check(self, operation, before_dispatch):
        async def check():
            await await_drained(before_dispatch())
            self._check_operation(operation)

        return check

    async def _post_response(self, operation, callback):
        if callback is not None:
            await await_drained(callback())
        self._check_operation("run_read" if operation == "run_create" else operation)

    def _remember_response(self, response, *, on_failed_close=None):
        self._responses.setdefault(response, {"attempted": False, "confirmed": False, "task": None, "stream_closed": False, "close_failed": False, "on_failed_close": on_failed_close})

    async def _response_content(self, response):
        value = await response.aread()
        if response.is_closed:
            self._responses[response]["confirmed"] = True
        return value

    async def _close_response(self, response):
        state = self._responses.get(response)
        if state is None:
            return

        async def close():
            if not state["stream_closed"]:
                if not state["confirmed"]:
                    if state["attempted"] or response.is_closed:
                        # HTTPX marks closed before its stream has drained.
                        await response.stream.aclose()
                    else:
                        state["attempted"] = True
                        await response.aclose()
                state["stream_closed"] = True
            if state["close_failed"] and state["on_failed_close"] is not None:
                await state["on_failed_close"]()
            self._responses.pop(response, None)

        if state["task"] is None or state["task"].done():
            state["task"] = asyncio.create_task(close(), name="channel-response-cleanup")
        try:
            await await_drained(self._join(state["task"]))
        except Exception as error:
            state["close_failed"] = True
            _discard_error_frames(error)
            raise ChannelExecutionProtocolError() from None

    async def _headers_for(self, owner, headers, *, streaming=False, replay=False):
        if type(owner) is not str or not owner or any(ord(c) <= 32 or ord(c) == 127 for c in owner):
            raise ChannelExecutionProtocolError()
        extra = _extra_headers(headers, streaming=streaming, replay=replay)
        authority = await await_drained(asyncio.to_thread(_authority_headers, owner))
        return {**authority, **extra, "Accept": "text/event-stream" if streaming else "application/json", "Content-Type": "application/json"}

    async def request_json(
        self,
        *,
        operation: ChannelOperation,
        owner_user_id: str,
        method: Literal["GET", "POST"],
        path: str,
        before_dispatch: BeforeDispatch,
        after_response: BeforeDispatch | None = None,
        body: Mapping[str, JsonValue] | None = None,
        params: Mapping[str, QueryValue] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> ChannelJsonResponse:
        self._check_operation(operation)
        path = _path(path)
        if method != ("POST" if operation in {"run_create", "run_cancel"} else "GET") or not callable(before_dispatch) or after_response is not None and not callable(after_response) or body is not None and not isinstance(body, Mapping):
            raise ChannelExecutionProtocolError()
        if params is not None and (not isinstance(params, Mapping) or any(type(k) is not str or type(v) not in {str, int, bool} for k, v in params.items())):
            raise ChannelExecutionProtocolError()

        async def run():
            try:
                await self.start()
                encoded = await await_drained(asyncio.to_thread(_encode, body)) if body is not None else None
                fresh_headers = await self._headers_for(owner_user_id, headers)
                self._check_operation(operation)
                context = _BEFORE_DISPATCH.set(self._dispatch_check(operation, before_dispatch))
                try:
                    response = await self._http_client.send(
                        self._http_client.build_request(method, path, content=encoded, params=params, headers=fresh_headers, timeout=httpx.Timeout(connect=5, pool=5, read=300, write=300) if operation == "run_create" else httpx.Timeout(30)),
                        stream=True,
                    )
                    self._remember_response(response)
                    try:
                        content = await self._response_content(response)
                        result = await await_drained(asyncio.to_thread(_json_response, response.status_code, _response_headers(response), content))
                        await self._post_response(operation, after_response)
                        return result
                    finally:
                        await self._close_response(response)
                finally:
                    _BEFORE_DISPATCH.reset(context)
            except asyncio.CancelledError:
                raise
            except Exception:
                raise ChannelExecutionProtocolError() from None

        task = self._register(run(), "channel-http-json")
        try:
            result = await asyncio.shield(task)
            self._check_operation("run_read" if operation == "run_create" else operation)
            return result
        except asyncio.CancelledError:
            task.cancel()
            await await_drained(self._join_retired(task))
            if self._stopping:
                await await_drained(self.stop())
            raise

    @staticmethod
    async def _join_retired(task):
        await asyncio.gather(task, return_exceptions=True)

    def stream_sse(self, *, owner_user_id: str, path: str, body: Mapping[str, JsonValue], before_dispatch: BeforeDispatch, after_response: BeforeDispatch | None = None, headers: Mapping[str, str] | None = None) -> ChannelOwnedStream:
        self._check_operation("run_create")
        if not isinstance(body, Mapping) or not callable(before_dispatch) or after_response is not None and not callable(after_response):
            raise ChannelExecutionProtocolError()
        return _OwnedStream(self, {"owner_user_id": owner_user_id, "path": _path(path), "body": body, "before_dispatch": before_dispatch, "after_response": after_response, "headers": headers})

    def join_sse(self, *, owner_user_id: str, path: str, before_dispatch: BeforeDispatch, after_response: BeforeDispatch | None = None, after_event_id: str | None = None) -> ChannelOwnedStream:
        self._check_operation("run_read")
        if not callable(before_dispatch) or after_response is not None and not callable(after_response):
            raise ChannelExecutionProtocolError() from None
        headers = {} if after_event_id is None else {"Last-Event-ID": after_event_id}
        _extra_headers(headers, streaming=True, replay=True)
        return _OwnedStream(self, {"operation": "run_read", "owner_user_id": owner_user_id, "path": _path(path), "before_dispatch": before_dispatch, "after_response": after_response, "headers": headers})

    def close_run_admission(self) -> None:
        self._run_admission_closed = True

    def begin_shutdown(self) -> None:
        self._stopping = True
        self.close_run_admission()
        for task in tuple(self._operations):
            task.cancel()

    async def _finish_stop(self) -> None:
        if self._construction is not None:
            await asyncio.gather(self._construction, return_exceptions=True)
        if self._requests:
            await asyncio.gather(*tuple(self._requests), return_exceptions=True)
        if self._streams:
            await asyncio.gather(*(stream.aclose() for stream in tuple(self._streams)))
        if self._operations:
            await asyncio.gather(*tuple(self._operations), return_exceptions=True)
        if self._responses:
            await asyncio.gather(*(self._close_response(response) for response in tuple(self._responses)))
        if self._client is not None:
            await self._client.aclose()
        self._closed = True

    async def stop(self) -> None:
        self.begin_shutdown()
        if self._stop_task is None or self._stop_task.done() and not self._closed:
            self._stop_task = asyncio.create_task(self._finish_stop(), name="telegram-thread-client-cleanup")
        await asyncio.shield(self._stop_task)


def gateway_client() -> GatewayClientOwner:
    from app.telegram_bots.supervisor import current

    supervisor = current()
    if supervisor is None or supervisor.thread_client is None:
        raise RuntimeError("thread-client-unavailable")
    return supervisor.thread_client


def gateway_url() -> str:
    return gateway_client().gateway_url


def owner_headers(owner_user_id: str) -> dict[str, str]:
    return gateway_client().owner_headers(owner_user_id)
