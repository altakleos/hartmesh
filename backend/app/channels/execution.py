"""Process-local channel capabilities and detached transport contracts.

Only trusted host code issues these objects. They are not provider metadata or
persisted run receipts, and this module owns no HTTP client or execution runtime.
"""

from __future__ import annotations

import re
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Literal, Protocol

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]
type QueryValue = str | int | bool
type ChannelOperation = Literal["thread_read", "run_create", "run_read", "source_read", "run_cancel"]
type BeforeDispatch = Callable[[], Awaitable[None]]


class ChannelExecutionProtocolError(RuntimeError):
    def __init__(self) -> None:
        super().__init__("Channel execution protocol refused")


class ChannelExecutionBusyError(RuntimeError):
    def __init__(self) -> None:
        super().__init__("Channel execution thread busy")


@dataclass(frozen=True)
class ChannelExecutionScope:
    publication_id: str
    owner_user_id: str
    connection_id: str | None
    chat_id: str
    thread_id: str
    endpoint_id: str
    assistant_id: str
    agent_name: str | None

    def __post_init__(self) -> None:
        if type(self.publication_id) is not str or re.fullmatch(r"[0-9a-f]{32}", self.publication_id) is None:
            raise ChannelExecutionProtocolError()
        for value in (self.owner_user_id, self.chat_id, self.thread_id, self.endpoint_id, self.assistant_id):
            if type(value) is not str or not value or value != value.strip() or any(not character.isprintable() for character in value):
                raise ChannelExecutionProtocolError()
        for value in (self.connection_id, self.agent_name):
            if value is not None and (type(value) is not str or not value or value != value.strip() or any(not character.isprintable() for character in value)):
                raise ChannelExecutionProtocolError()


class ChannelRunAdmission(Protocol):
    async def authorize(self, operation: ChannelOperation) -> None: ...

    def current(self, operation: ChannelOperation) -> bool: ...


@dataclass(frozen=True)
class ChannelExecutionContext:
    scope: ChannelExecutionScope
    client: ChannelExecutionClient = field(repr=False)
    admission: ChannelRunAdmission = field(repr=False)

    def __post_init__(self) -> None:
        if type(self.scope) is not ChannelExecutionScope:
            raise ChannelExecutionProtocolError()

    @property
    def publication_id(self) -> str:
        return self.scope.publication_id

    @property
    def owner_user_id(self) -> str:
        return self.scope.owner_user_id

    @property
    def connection_id(self) -> str | None:
        return self.scope.connection_id

    @property
    def chat_id(self) -> str:
        return self.scope.chat_id

    @property
    def thread_id(self) -> str:
        return self.scope.thread_id

    @property
    def endpoint_id(self) -> str:
        return self.scope.endpoint_id

    @property
    def assistant_id(self) -> str:
        return self.scope.assistant_id

    @property
    def agent_name(self) -> str | None:
        return self.scope.agent_name


@dataclass(frozen=True)
class ChannelJsonResponse:
    status_code: int
    headers: Mapping[str, str] = field(repr=False)
    body: JsonValue = field(repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "headers", MappingProxyType(dict(self.headers)))
        object.__setattr__(self, "body", deepcopy(self.body))


class ChannelHttpError(RuntimeError):
    def __init__(self, response: ChannelJsonResponse) -> None:
        super().__init__("Channel execution HTTP request failed")
        self._response = ChannelJsonResponse(response.status_code, response.headers, response.body)

    @property
    def response(self) -> ChannelJsonResponse:
        return self._response


class ChannelStreamPart(Protocol):
    @property
    def event(self) -> str: ...

    @property
    def data(self) -> Any: ...

    @property
    def id(self) -> str | None: ...


class ChannelOwnedStream(Protocol):
    def __aiter__(self) -> AsyncIterator[ChannelStreamPart]: ...

    async def __anext__(self) -> ChannelStreamPart: ...

    async def aclose(self) -> None: ...


class ChannelHttpOwner(Protocol):
    @property
    def endpoint_id(self) -> str: ...

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
    ) -> ChannelJsonResponse: ...

    def stream_sse(
        self,
        *,
        owner_user_id: str,
        path: str,
        body: Mapping[str, JsonValue],
        before_dispatch: BeforeDispatch,
        after_response: BeforeDispatch | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> ChannelOwnedStream: ...

    def join_sse(
        self,
        *,
        owner_user_id: str,
        path: str,
        before_dispatch: BeforeDispatch,
        after_response: BeforeDispatch | None = None,
        after_event_id: str | None = None,
    ) -> ChannelOwnedStream: ...


class ChannelExecutionThreads(Protocol):
    async def get(self, thread_id: str, *, headers: Mapping[str, str] | None = None) -> dict: ...


class ChannelExecutionRuns(Protocol):
    async def wait(
        self,
        thread_id: str,
        assistant_id: str,
        *,
        input: dict,
        config: dict | None = None,
        context: dict | None = None,
        metadata: dict | None = None,
        multitask_strategy: Literal["reject"] = "reject",
        channel_execution: Mapping[str, JsonValue],
        on_disconnect: Literal["continue"] = "continue",
        on_completion: None = None,
        headers: Mapping[str, str] | None = None,
    ) -> JsonValue: ...

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
        multitask_strategy: Literal["reject"] = "reject",
        channel_execution: Mapping[str, JsonValue],
        on_disconnect: Literal["continue"] = "continue",
        on_completion: None = None,
        headers: Mapping[str, str] | None = None,
    ) -> ChannelOwnedStream: ...


class ChannelExecutionClient(Protocol):
    @property
    def threads(self) -> ChannelExecutionThreads: ...

    @property
    def runs(self) -> ChannelExecutionRuns: ...
