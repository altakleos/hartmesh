"""One loop-owned HTTP/SDK client for owner-scoped conversation preparation."""

from __future__ import annotations

import asyncio
from contextvars import ContextVar
from hashlib import sha256
from typing import TYPE_CHECKING

import httpx

from deerflow.utils.file_io import await_drained

if TYPE_CHECKING:
    from ssl import SSLContext

_BEFORE_DISPATCH = ContextVar("telegram_thread_before_dispatch", default=None)


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

    from app.gateway.agent_discovery import THREAD_BINDING_HEADER_NAME
    from app.gateway.csrf_middleware import CSRF_COOKIE_NAME, CSRF_HEADER_NAME, generate_csrf_token
    from app.gateway.internal_auth import create_internal_auth_headers

    csrf = generate_csrf_token()
    headers = {**create_internal_auth_headers(), CSRF_HEADER_NAME: csrf, "Cookie": f"{CSRF_COOKIE_NAME}={csrf}"}
    client = httpx.AsyncClient(base_url=base_url + "/", verify=tls_context if tls_context is not None else True, trust_env=False, timeout=httpx.Timeout(30), transport=transport, event_hooks={"request": [_before_request]})
    return LangGraphClient(client), headers, THREAD_BINDING_HEADER_NAME


class GatewayClientOwner:
    def __init__(self, *, gateway_url: str, langgraph_url: str, tls_context: SSLContext | None = None, transport=None):
        self._gateway_url = gateway_url.rstrip("/")
        self._base_url = _endpoint(langgraph_url)
        self._endpoint_id = sha256(self._base_url.encode("utf-8")).hexdigest()
        self._tls_context, self._transport = tls_context, transport
        self._client, self._headers = None, None
        self._binding_header = None
        self._construction: asyncio.Task | None = None
        self._stop_task: asyncio.Task | None = None
        self._requests: set[asyncio.Task] = set()
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
        self._client, self._headers, self._binding_header = await asyncio.to_thread(_construct, self._base_url, self._tls_context, self._transport)

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

    def begin_shutdown(self) -> None:
        self._stopping = True

    async def _finish_stop(self) -> None:
        if self._construction is not None:
            await asyncio.gather(self._construction, return_exceptions=True)
        if self._requests:
            await asyncio.gather(*tuple(self._requests), return_exceptions=True)
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
