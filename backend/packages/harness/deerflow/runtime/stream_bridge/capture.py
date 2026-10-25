"""Per-run producer ownership without acquiring the shared bridge client."""

from __future__ import annotations

from typing import Any

from deerflow.config.stream_bridge_config import DEFAULT_HEARTBEAT_INTERVAL_SECONDS
from deerflow.runtime.runs.capture_io import CaptureIOClosed, CaptureIOOwner, CaptureIOUnsupported, current_capture_io_owner

from .base import StreamBridge


class _CapturedStreamProducer(StreamBridge):
    def __init__(self, bridge: StreamBridge, owner: CaptureIOOwner) -> None:
        super().__init__(heartbeat_interval=getattr(bridge, "heartbeat_interval", DEFAULT_HEARTBEAT_INTERVAL_SECONDS))
        self._bridge = bridge
        self._owner = owner
        self._retired = False
        self.supports_cross_process = bridge.supports_cross_process
        owner.register_resource(self)

    def _check_open(self) -> None:
        current = current_capture_io_owner()
        if current is not None and current is not self._owner:
            raise CaptureIOUnsupported() from None
        self._owner.check_open()
        if self._retired:
            raise CaptureIOClosed() from None

    async def publish(self, run_id: str, event: str, data: Any) -> None:
        self._check_open()

        def invoke():
            self._check_open()
            return self._bridge.publish(run_id, event, data)

        await self._owner.run(invoke)

    async def publish_end(self, run_id: str) -> None:
        self._check_open()

        def invoke():
            self._check_open()
            return self._bridge.publish_end(run_id)

        await self._owner.run(invoke)

    def subscribe(self, run_id: str, *, last_event_id: str | None = None, heartbeat_interval: float | None = None):
        return self._bridge.subscribe(run_id, last_event_id=last_event_id, heartbeat_interval=heartbeat_interval)

    async def cleanup(self, run_id: str, *, delay: float = 0) -> None:
        self._check_open()
        # Retention delays and stream deletion belong to the application owner.
        raise CaptureIOUnsupported() from None

    def retire(self) -> None:
        self._retired = True

    async def drain(self) -> None:
        if self._retired:
            self._owner.unregister_resource(self)

    async def close(self) -> None:
        self.retire()
        await self.drain()


def capture_stream_producer(bridge: StreamBridge, owner: CaptureIOOwner | None) -> StreamBridge:
    """Freeze producer admission to the original run; ordinary callers keep identity."""
    if owner is None:
        return bridge
    owner.check_open()
    if isinstance(bridge, _CapturedStreamProducer):
        if bridge._owner is not owner:
            raise CaptureIOUnsupported() from None
        bridge._check_open()
        return bridge
    return _CapturedStreamProducer(bridge, owner)
