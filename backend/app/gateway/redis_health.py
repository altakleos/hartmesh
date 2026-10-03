"""Coalesced readiness of required Redis clients; no keys or credentials in evidence."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from app.gateway.auth.login_throttle import get_login_throttle_store
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)
_TIMEOUT_SECONDS = 2.0
_CACHE_SECONDS = 1.0


def _local_config():
    from app.gateway.routers.auth import _login_local_config

    return _login_local_config()


class RequiredRedisReadiness:
    """Borrow startup streaming and live login clients; own only one probe task.

    Waiters have a bounded deadline. A slow config worker remains owned until
    drained, including after that deadline, so public requests cannot pile up
    workers or make a cancelled worker select a stale login backend later.
    Completed results are reused for one second to bound probe traffic.
    """

    def __init__(self, bridge: Any, *, local_config: Callable = _local_config, tenant_namespace: Any = None):
        self._bridge = bridge
        self._local_config = local_config
        self._tenant_namespace = tenant_namespace
        self._task: asyncio.Task[str] | None = None
        self._next_probe = 0.0
        self._closed = False

    async def check(self) -> str:
        if self._closed:
            return "unreachable"
        if self._task is None or (self._task.done() and asyncio.get_running_loop().time() >= self._next_probe):
            self._task = asyncio.create_task(self._attempt(), name="required-redis-readiness")
        try:
            async with asyncio.timeout(_TIMEOUT_SECONDS):
                return await asyncio.shield(self._task)
        except TimeoutError:
            return "unreachable"

    async def _attempt(self) -> str:
        try:
            async with asyncio.timeout(_TIMEOUT_SECONDS):
                local = await await_drained(asyncio.to_thread(self._local_config))
                targets = []
                # This is the actual bridge, never a hot-reloaded replacement.
                if self._bridge is None:
                    return "unreachable"
                stream_probe = getattr(self._bridge, "check_health", None)
                if stream_probe is not None:
                    targets.append(stream_probe)
                if local.enabled and local.lockout_store == "redis":
                    store = get_login_throttle_store(local, tenant_namespace=self._tenant_namespace)
                    targets.append(store.check_health)
                if not targets:
                    return "not_configured"
                results = await asyncio.gather(*(self._ping(target) for target in targets))
                return "ok" if all(results) else "unreachable"
        except Exception:
            # Config/Redis exceptions can contain credential-bearing URLs.
            logger.warning("Required Redis readiness probe failed")
            return "unreachable"
        finally:
            self._next_probe = asyncio.get_running_loop().time() + _CACHE_SECONDS

    @staticmethod
    async def _ping(target: Callable) -> bool:
        try:
            return bool(await target())
        except Exception:
            logger.warning("Required Redis readiness probe failed")
            return False

    async def close(self) -> None:
        self._closed = True
        if self._task is not None:
            self._task.cancel()

            async def drain():
                await asyncio.gather(self._task, return_exceptions=True)

            await await_drained(drain())
