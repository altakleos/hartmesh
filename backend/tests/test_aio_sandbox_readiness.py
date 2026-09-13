from __future__ import annotations

from types import SimpleNamespace

import pytest

from deerflow.community.aio_sandbox import backend as readiness


class _FakeAsyncClient:
    def __init__(
        self,
        *,
        responses: list[object],
        calls: list[str],
        timeout: float,
        request_timeouts: list[float] | None = None,
        trust_env: bool = True,
    ) -> None:
        self._responses = responses
        self._calls = calls
        self._timeout = timeout
        self._request_timeouts = request_timeouts
        self.trust_env = trust_env

    async def __aenter__(self) -> _FakeAsyncClient:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def get(self, url: str, *, timeout: float):
        self._calls.append(url)
        if self._request_timeouts is not None:
            self._request_timeouts.append(timeout)
        response = self._responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


class _FakeLoop:
    def __init__(self, times: list[float]) -> None:
        self._times = times
        self._index = 0

    def time(self) -> float:
        value = self._times[self._index]
        self._index += 1
        return value


@pytest.mark.parametrize(
    ("sandbox_url", "expected"),
    [
        ("http://localhost:8080", False),
        ("http://127.0.0.1:8080", False),
        ("http://[::1]:8080", False),
        ("http://host.docker.internal:8080", False),
        ("http://host.containers.internal:8080", False),
        ("http://k3s:30001", False),
        ("http://10.0.0.8:8080", False),
        ("http://8.8.8.8:8080", True),
        ("http://[2606:4700:4700::1111]:8080", True),
        ("https://sandbox.example.com", True),
    ],
)
def test_sandbox_http_trust_env_only_uses_proxy_for_external_urls(sandbox_url: str, expected: bool) -> None:
    assert readiness.sandbox_http_trust_env(sandbox_url) is expected


def test_wait_for_sandbox_ready_bypasses_environment_proxy_for_docker_host(monkeypatch: pytest.MonkeyPatch) -> None:
    sessions: list[object] = []

    class FakeSession:
        trust_env = True

        def __init__(self) -> None:
            self.headers: dict[str, str] = {}

        def __enter__(self):
            sessions.append(self)
            return self

        def __exit__(self, *_exc_info) -> None:
            return None

        def get(self, url: str, *, timeout: float):
            assert url == "http://host.docker.internal:8080/v1/sandbox"
            assert 0 < timeout <= 1, "each request is clamped to what is left of the one-second budget"
            return SimpleNamespace(status_code=200)

    monkeypatch.setattr(readiness.requests, "Session", FakeSession)

    headers = {"X-DeerFlow-Relay-Token": "secret-token"}
    assert (
        readiness.wait_for_sandbox_ready(
            "http://host.docker.internal:8080",
            timeout=1,
            headers=headers,
        )
        is True
    )
    assert len(sessions) == 1
    assert sessions[0].trust_env is False
    assert sessions[0].headers == headers


@pytest.mark.anyio
async def test_wait_for_sandbox_ready_async_uses_nonblocking_polling(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    sleeps: list[float] = []
    clients: list[_FakeAsyncClient] = []
    client_headers: list[dict[str, str]] = []

    def fake_client(*, timeout: float, trust_env: bool, headers: dict[str, str]):
        client_headers.append(headers)
        client = _FakeAsyncClient(
            responses=[SimpleNamespace(status_code=503), SimpleNamespace(status_code=200)],
            calls=calls,
            timeout=timeout,
            trust_env=trust_env,
        )
        clients.append(client)
        return client

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(readiness.httpx, "AsyncClient", fake_client)
    monkeypatch.setattr(readiness.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(readiness.requests, "get", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("requests.get should not be used")))
    monkeypatch.setattr(readiness.time, "sleep", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("time.sleep should not be used")))

    headers = {"X-DeerFlow-Relay-Token": "secret-token"}
    assert (
        await readiness.wait_for_sandbox_ready_async(
            "http://sandbox",
            timeout=5,
            poll_interval=0.05,
            headers=headers,
        )
        is True
    )

    assert calls == ["http://sandbox/v1/sandbox", "http://sandbox/v1/sandbox"]
    assert sleeps == [0.05]
    assert clients[0].trust_env is False
    assert client_headers == [headers]


@pytest.mark.anyio
async def test_wait_for_sandbox_ready_async_retries_request_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    sleeps: list[float] = []

    def fake_client(*, timeout: float, trust_env: bool):
        return _FakeAsyncClient(
            responses=[readiness.httpx.ConnectError("not ready"), SimpleNamespace(status_code=200)],
            calls=calls,
            timeout=timeout,
            trust_env=trust_env,
        )

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(readiness.httpx, "AsyncClient", fake_client)
    monkeypatch.setattr(readiness.asyncio, "sleep", fake_sleep)

    assert await readiness.wait_for_sandbox_ready_async("http://sandbox", timeout=5, poll_interval=0.01) is True

    assert len(calls) == 2
    assert sleeps == [0.01]


@pytest.mark.anyio
async def test_wait_for_sandbox_ready_async_clamps_request_and_sleep_to_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    request_timeouts: list[float] = []
    sleeps: list[float] = []

    def fake_client(*, timeout: float, trust_env: bool):
        return _FakeAsyncClient(
            responses=[SimpleNamespace(status_code=503)],
            calls=calls,
            timeout=timeout,
            request_timeouts=request_timeouts,
            trust_env=trust_env,
        )

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(readiness.httpx, "AsyncClient", fake_client)
    monkeypatch.setattr(readiness.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(readiness.asyncio, "get_running_loop", lambda: _FakeLoop([100.0, 100.5, 101.75, 102.0]))

    assert await readiness.wait_for_sandbox_ready_async("http://sandbox", timeout=2, poll_interval=1.0) is False

    assert calls == ["http://sandbox/v1/sandbox"]
    assert request_timeouts == [1.5]
    assert sleeps == [0.25]


# ── The deadline is a deadline (P-z) ───────────────────────────────────────


class _RecordingSession:
    """A ``requests.Session`` stand-in that records request timeouts."""

    trust_env = True
    instances: list[_RecordingSession] = []

    def __init__(self, responses: list[object]) -> None:
        self.headers: dict[str, str] = {}
        self.responses = responses
        self.timeouts: list[float] = []
        _RecordingSession.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *_exc_info) -> None:
        return None

    def get(self, url: str, *, timeout: float):
        self.timeouts.append(timeout)
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


def _install_session(monkeypatch: pytest.MonkeyPatch, responses: list[object]) -> _RecordingSession:
    session = _RecordingSession(responses)
    monkeypatch.setattr(readiness.requests, "Session", lambda: session)
    return session


class _Clock:
    """A monotonic source the test advances by hand."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.mark.parametrize("bad", [0, -1, -0.5, float("inf"), float("-inf"), float("nan"), True, False, "60", None, [60], readiness.SANDBOX_READY_TIMEOUT_MAX + 1])
def test_normalize_ready_timeout_refuses_a_value_that_would_disable_or_malform_the_deadline(bad: object) -> None:
    with pytest.raises(ValueError, match="readiness budget"):
        readiness.normalize_ready_timeout(bad)


@pytest.mark.parametrize(("value", "expected"), [(120, 120.0), (0.5, 0.5), (readiness.SANDBOX_READY_TIMEOUT_MAX, float(readiness.SANDBOX_READY_TIMEOUT_MAX))])
def test_normalize_ready_timeout_accepts_finite_positive_seconds(value: object, expected: float) -> None:
    assert readiness.normalize_ready_timeout(value) == expected


def test_default_budget_is_within_the_supported_range() -> None:
    assert readiness.normalize_ready_timeout(readiness.SANDBOX_LOCAL_PROVIDER_READY_TIMEOUT) == 60.0


@pytest.mark.parametrize("bad", [0, -1, float("inf"), float("nan"), True])
def test_wait_for_sandbox_ready_refuses_a_budget_that_would_disable_the_deadline(monkeypatch: pytest.MonkeyPatch, bad: object) -> None:
    session = _install_session(monkeypatch, [SimpleNamespace(status_code=200)])
    with pytest.raises(ValueError, match="readiness budget"):
        readiness.wait_for_sandbox_ready("http://sandbox", timeout=bad)
    assert session.timeouts == [], "no request may be sent under an invalid budget"


def test_wait_for_sandbox_ready_polls_on_a_monotonic_clock_and_clamps_requests_and_sleeps_to_the_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    """A wall-clock jump must not extend or shorten the wait, and neither a
    request nor a sleep may reach past the deadline."""
    clock = _Clock()
    monkeypatch.setattr(readiness, "_monotonic", clock)
    monkeypatch.setattr(readiness.time, "sleep", clock.sleep)
    monkeypatch.setattr(readiness.time, "time", lambda: (_ for _ in ()).throw(AssertionError("wall clock must not be consulted")))

    def slow_503():
        clock.now += 4.0  # each probe costs four seconds of the budget
        return SimpleNamespace(status_code=503)

    responses: list[object] = []
    session = _install_session(monkeypatch, responses)
    session.get = lambda url, *, timeout: (session.timeouts.append(timeout), slow_503())[1]  # type: ignore[method-assign]

    assert readiness.wait_for_sandbox_ready("http://sandbox", timeout=7) is False

    # t=0: 7 left -> request capped at 5 (took 4); t=4: sleep min(1, 3)=1;
    # t=5: 2 left -> request capped at 2 (took 4, now t=9): deadline passed.
    assert session.timeouts == [5.0, 2.0]
    assert clock.sleeps == [1.0]


def test_wait_for_sandbox_ready_does_not_accept_a_response_that_arrives_after_the_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = _Clock()
    monkeypatch.setattr(readiness, "_monotonic", clock)
    monkeypatch.setattr(readiness.time, "sleep", clock.sleep)
    session = _install_session(monkeypatch, [])

    def late_200(url, *, timeout):
        session.timeouts.append(timeout)
        clock.now += timeout + 0.5  # the reply lands after the deadline
        return SimpleNamespace(status_code=200)

    session.get = late_200  # type: ignore[method-assign]

    assert readiness.wait_for_sandbox_ready("http://sandbox", timeout=3) is False
    assert session.timeouts == [3.0]
    assert clock.sleeps == []


def test_wait_for_sandbox_ready_accepts_a_response_that_arrives_on_time(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = _Clock()
    monkeypatch.setattr(readiness, "_monotonic", clock)
    monkeypatch.setattr(readiness.time, "sleep", clock.sleep)
    session = _install_session(monkeypatch, [readiness.requests.exceptions.ConnectionError("refused"), SimpleNamespace(status_code=200)])

    assert readiness.wait_for_sandbox_ready("http://sandbox", timeout=30) is True
    assert session.timeouts == [5.0, 5.0]
    assert clock.sleeps == [1.0]


@pytest.mark.anyio
@pytest.mark.parametrize("bad", [0, -1, float("inf"), float("nan"), True])
async def test_wait_for_sandbox_ready_async_refuses_a_budget_that_would_disable_the_deadline(monkeypatch: pytest.MonkeyPatch, bad: object) -> None:
    monkeypatch.setattr(readiness.httpx, "AsyncClient", lambda **_kwargs: (_ for _ in ()).throw(AssertionError("no client may be built under an invalid budget")))
    with pytest.raises(ValueError, match="readiness budget"):
        await readiness.wait_for_sandbox_ready_async("http://sandbox", timeout=bad)


@pytest.mark.anyio
async def test_wait_for_sandbox_ready_async_bounds_a_stalled_request_to_the_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    """httpx timeouts are per phase, so a reply that drips forever would never
    trip them; the whole request is bounded by what is left of the budget."""
    import asyncio

    started = asyncio.Event()
    cancelled = asyncio.Event()

    class _HangingClient:
        def __init__(self, **_kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc_info) -> None:
            return None

        async def get(self, url: str, *, timeout: float):
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

    monkeypatch.setattr(readiness.httpx, "AsyncClient", _HangingClient)
    loop = asyncio.get_running_loop()
    began = loop.time()
    assert await readiness.wait_for_sandbox_ready_async("http://sandbox", timeout=0.2) is False
    assert loop.time() - began < 2.0
    assert started.is_set() and cancelled.is_set(), "the stalled request must be torn down at the deadline"


@pytest.mark.anyio
async def test_wait_for_sandbox_ready_async_does_not_accept_a_response_that_arrives_after_the_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    sleeps: list[float] = []

    def fake_client(*, timeout: float, trust_env: bool):
        return _FakeAsyncClient(responses=[SimpleNamespace(status_code=200)], calls=calls, timeout=timeout, trust_env=trust_env)

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(readiness.httpx, "AsyncClient", fake_client)
    monkeypatch.setattr(readiness.asyncio, "sleep", fake_sleep)
    # deadline = 100 + 2; the request starts at 100.5 and its 200 lands at 102.6.
    monkeypatch.setattr(readiness.asyncio, "get_running_loop", lambda: _FakeLoop([100.0, 100.5, 102.6]))

    assert await readiness.wait_for_sandbox_ready_async("http://sandbox", timeout=2, poll_interval=1.0) is False
    assert calls == ["http://sandbox/v1/sandbox"]
    assert sleeps == []


@pytest.mark.anyio
async def test_wait_for_sandbox_ready_async_propagates_cancellation_while_a_request_hangs(monkeypatch: pytest.MonkeyPatch) -> None:
    import asyncio

    class _HangingClient:
        def __init__(self, **_kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc_info) -> None:
            return None

        async def get(self, url: str, *, timeout: float):
            await asyncio.Event().wait()

    monkeypatch.setattr(readiness.httpx, "AsyncClient", _HangingClient)
    task = asyncio.ensure_future(readiness.wait_for_sandbox_ready_async("http://sandbox", timeout=60))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=2.0)
