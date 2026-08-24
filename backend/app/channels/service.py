"""ChannelService — manages the lifecycle of all IM channels."""

from __future__ import annotations

import asyncio
import logging
import math
import os
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

from app.channels.base import Channel
from app.channels.manager import DEFAULT_CHANNEL_MAX_CONCURRENCY, DEFAULT_CHANNEL_SHUTDOWN_GRACE_PERIOD_SECONDS, DEFAULT_GATEWAY_URL, DEFAULT_LANGGRAPH_URL, ChannelManager
from app.channels.message_bus import DEFAULT_INBOUND_QUEUE_MAXSIZE, MessageBus
from app.channels.runtime_config_store import merge_runtime_channel_configs
from app.channels.store import ChannelStore

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from app.runtime import InvocationRuntime
    from deerflow.config.app_config import AppConfig
    from deerflow.config.channel_connections_config import ChannelConnectionsConfig
    from deerflow.runtime import StreamBridge

# Channel name → import path for lazy loading
_CHANNEL_REGISTRY: dict[str, str] = {
    "buzz": "app.channels.buzz:BuzzChannel",
    "dingtalk": "app.channels.dingtalk:DingTalkChannel",
    "discord": "app.channels.discord:DiscordChannel",
    "feishu": "app.channels.feishu:FeishuChannel",
    "github": "app.channels.github:GitHubChannel",
    "slack": "app.channels.slack:SlackChannel",
    "telegram": "app.channels.telegram:TelegramChannel",
    "wechat": "app.channels.wechat:WechatChannel",
    "wecom": "app.channels.wecom:WeComChannel",
}

# Keys that indicate a user has configured credentials for a channel.
_CHANNEL_CREDENTIAL_KEYS: dict[str, list[str]] = {
    "buzz": ["private_key"],
    "dingtalk": ["client_id", "client_secret"],
    "discord": ["bot_token"],
    "feishu": ["app_id", "app_secret"],
    "slack": ["bot_token", "app_token"],
    "telegram": ["bot_token"],
    "wecom": ["bot_id", "bot_secret"],
    "wechat": ["bot_token"],
}

_CHANNELS_LANGGRAPH_URL_ENV = "DEER_FLOW_CHANNELS_LANGGRAPH_URL"
_CHANNELS_GATEWAY_URL_ENV = "DEER_FLOW_CHANNELS_GATEWAY_URL"


def _channel_has_credentials(name: str, channel_config: dict[str, Any]) -> bool:
    cred_keys = _CHANNEL_CREDENTIAL_KEYS.get(name, [])
    return any(not isinstance(channel_config.get(key), bool) and channel_config.get(key) is not None and str(channel_config[key]).strip() for key in cred_keys)


def _resolve_service_url(config: dict[str, Any], config_key: str, env_key: str, default: str) -> str:
    value = config.pop(config_key, None)
    if isinstance(value, str) and value.strip():
        return value
    env_value = os.getenv(env_key, "").strip()
    if env_value:
        return env_value
    return default


def _resolve_positive_int(config: dict[str, Any], config_key: str, default: int) -> int:
    value = config.pop(config_key, None)
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        logger.warning("Invalid channels.%s=%r; using default %d", config_key, value, default)
        return default
    return value


def _resolve_non_negative_float(config: dict[str, Any], config_key: str, default: float) -> float:
    value = config.pop(config_key, None)
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        logger.warning("Invalid channels.%s=%r; using default %.1f", config_key, value, default)
        return default
    return float(value)


def _merge_channel_connection_runtime_config(channels_config: dict[str, Any], app_config: AppConfig) -> None:
    connection_config = getattr(app_config, "channel_connections", None)
    merge_runtime_channel_configs(channels_config, connection_config)


def _make_connection_repo(connection_config: ChannelConnectionsConfig | None):
    if connection_config is None or not getattr(connection_config, "enabled", False):
        return None

    try:
        from deerflow.persistence.channel_connections import ChannelConnectionRepository
        from deerflow.persistence.engine import get_session_factory
    except Exception:
        logger.exception("Failed to import channel connection repository")
        return None

    session_factory = get_session_factory()
    if session_factory is None:
        logger.warning("Channel connections are enabled but database persistence is not available")
        return None
    return ChannelConnectionRepository(session_factory)


def _uses_postgres_receipts(app_config: AppConfig | None) -> bool:
    if app_config is None:
        return False
    database = getattr(app_config, "database", None)
    if getattr(database, "backend", None) != "postgres":
        return False
    dedupe = getattr(app_config, "dedupe_storage", None)
    backend = getattr(dedupe, "backend", "auto")
    raw_backend = backend.value if hasattr(backend, "value") else str(backend)
    return raw_backend != "memory"


class ChannelService:
    """Manages the lifecycle of all configured IM channels.

    Reads configuration from ``config.yaml`` under the ``channels`` key,
    instantiates enabled channels, and starts the ChannelManager dispatcher.
    """

    def __init__(
        self,
        channels_config: dict[str, Any] | None = None,
        *,
        connection_repo: Any | None = None,
        require_bound_identity: bool = False,
        app_config: AppConfig | None = None,
        get_stream_bridge: Callable[[], StreamBridge | None] | None = None,
        invocation_runtime: InvocationRuntime | None = None,
    ) -> None:
        config = dict(channels_config or {})
        inbound_queue_maxsize = _resolve_positive_int(config, "inbound_queue_maxsize", DEFAULT_INBOUND_QUEUE_MAXSIZE)
        max_concurrency = _resolve_positive_int(config, "max_concurrency", DEFAULT_CHANNEL_MAX_CONCURRENCY)
        shutdown_grace_period_seconds = _resolve_non_negative_float(config, "shutdown_grace_period_seconds", DEFAULT_CHANNEL_SHUTDOWN_GRACE_PERIOD_SECONDS)
        self.bus = MessageBus(inbound_queue_maxsize=inbound_queue_maxsize)
        self.store = ChannelStore()
        self._connection_repo = connection_repo
        self._get_stream_bridge = get_stream_bridge
        deployment = getattr(app_config, "deployment", None)
        self._durable_production = getattr(deployment, "profile", None) == "durable_production"
        langgraph_url = _resolve_service_url(config, "langgraph_url", _CHANNELS_LANGGRAPH_URL_ENV, DEFAULT_LANGGRAPH_URL)
        gateway_url = _resolve_service_url(config, "gateway_url", _CHANNELS_GATEWAY_URL_ENV, DEFAULT_GATEWAY_URL)
        default_session = config.pop("session", None)
        channel_sessions = {name: channel_config.get("session") for name, channel_config in config.items() if isinstance(channel_config, dict)}
        from app.channels.dedupe_store import make_inbound_dedupe_store

        self.manager = ChannelManager(
            bus=self.bus,
            store=self.store,
            max_concurrency=max_concurrency,
            shutdown_grace_period_seconds=shutdown_grace_period_seconds,
            langgraph_url=langgraph_url,
            gateway_url=gateway_url,
            default_session=default_session if isinstance(default_session, dict) else None,
            channel_sessions=channel_sessions,
            connection_repo=connection_repo,
            require_bound_identity=require_bound_identity,
            inbound_dedupe_store=make_inbound_dedupe_store(app_config),
            get_stream_bridge=get_stream_bridge,
            invocation_runtime=invocation_runtime,
        )
        self.inbound_receipt_processor = None
        self.inbound_receipt_operations = None
        if _uses_postgres_receipts(app_config):
            from app.channels.inbound_receipt_operations import InboundReceiptOperations
            from app.channels.inbound_receipts import (
                InboundReceiptProcessor,
                InboundReceiptWakeup,
                SqlInboundReceiptStore,
            )
            from deerflow.persistence.engine import get_session_factory

            session_factory = get_session_factory()
            if session_factory is None:
                raise RuntimeError("PostgreSQL inbound receipt storage is unavailable")
            receipt_store = SqlInboundReceiptStore(session_factory)
            self.inbound_receipt_processor = InboundReceiptProcessor(
                store=receipt_store,
                publish_wakeup=self.bus.publish_receipt_wakeup,
                process_message=self.manager.process_inbound_receipt_message,
            )

            async def publish_operator_wakeup(receipt_id: str) -> None:
                await self.bus.publish_receipt_wakeup(InboundReceiptWakeup(receipt_id))

            self.inbound_receipt_operations = InboundReceiptOperations(
                store=receipt_store,
                publish_wakeup=publish_operator_wakeup,
            )
            self.manager.set_inbound_receipt_processor(self.inbound_receipt_processor)
        self._channels: dict[str, Any] = {}  # name -> Channel instance
        self._config = config
        self._running = False
        self._readiness_locks: dict[str, asyncio.Lock] = {}

    @classmethod
    def from_app_config(
        cls,
        app_config: AppConfig | None = None,
        *,
        get_stream_bridge: Callable[[], StreamBridge | None] | None = None,
        invocation_runtime: InvocationRuntime | None = None,
    ) -> ChannelService:
        """Create a ChannelService from the application config.

        ``get_stream_bridge`` is threaded straight through to the
        ``ChannelManager`` (see its docstring); it is optional so direct
        callers (including most tests) that don't need follow-up-buffer
        auto-draining can omit it. The embedded Gateway also supplies its
        explicitly constructed ``InvocationRuntime``; omission retains the
        standalone SDK transport boundary.
        """
        if app_config is None:
            from deerflow.config.app_config import get_app_config

            app_config = get_app_config()
        channels_config = {}
        # extra fields are allowed by AppConfig (extra="allow")
        extra = app_config.model_extra or {}
        if "channels" in extra:
            channels_config = dict(extra["channels"] or {})
        _merge_channel_connection_runtime_config(channels_config, app_config)
        connection_config = getattr(app_config, "channel_connections", None)
        connections_enabled = connection_config is not None and getattr(connection_config, "enabled", False)
        require_bound_identity = bool(connections_enabled and getattr(connection_config, "require_bound_identity", True))
        return cls(
            channels_config=channels_config,
            connection_repo=_make_connection_repo(connection_config),
            require_bound_identity=require_bound_identity,
            app_config=app_config,
            get_stream_bridge=get_stream_bridge,
            invocation_runtime=invocation_runtime,
        )

    async def start(self) -> None:
        """Start the manager and all enabled channels."""
        if self._running:
            return

        await self.manager.start()
        if self.inbound_receipt_processor is not None:
            await self.inbound_receipt_processor.start()
        self._running = True

        ready_status = await self.ensure_ready_channels(attempts=2)
        ready_count = sum(1 for ready in ready_status.values() if ready)
        logger.info("ChannelService started with %d/%d ready channels", ready_count, len(ready_status))

    async def ensure_ready_channels(self, *, attempts: int = 1) -> dict[str, bool]:
        """Start or restart enabled configured channels that are not ready."""
        ready_status: dict[str, bool] = {}
        for name, channel_config in self._config.items():
            if not isinstance(channel_config, dict):
                continue
            if not channel_config.get("enabled", False):
                if _channel_has_credentials(name, channel_config):
                    logger.warning(
                        "A configured channel has credentials configured but is disabled. Set enabled: true under its channels entry in config.yaml to activate it.",
                    )
                else:
                    logger.info("A configured channel is disabled, skipping")
                continue

            ready_status[name] = await self.ensure_channel_ready(name, attempts=attempts)
        return ready_status

    async def ensure_channel_ready(
        self,
        name: str,
        config: dict[str, Any] | None = None,
        *,
        attempts: int = 1,
    ) -> bool:
        """Ensure a single enabled channel is running using its current config."""
        if not self._running:
            logger.warning("ChannelService is not running; cannot ensure channel readiness")
            return False

        if config is not None:
            self._config[name] = dict(config)

        # Serialize per channel: readiness is polled from request handlers, so
        # concurrent calls must not stop/start the same channel worker twice.
        lock = self._readiness_locks.setdefault(name, asyncio.Lock())
        async with lock:
            channel_config = self._config.get(name)
            if not channel_config or not isinstance(channel_config, dict):
                logger.warning("No config for requested channel")
                return False
            if not channel_config.get("enabled", False):
                return False

            channel = self._channels.get(name)
            if channel is not None and channel.is_running:
                return True

            if channel is not None:
                try:
                    await channel.stop()
                except Exception:
                    logger.exception("Error stopping non-running channel before readiness retry")
                self._channels.pop(name, None)

            max_attempts = max(1, attempts)
            for attempt in range(max_attempts):
                if attempt > 0:
                    logger.info("Retrying channel startup after readiness check")
                if await self._start_channel(name, channel_config):
                    return True
            return False

    async def stop(self) -> None:
        """Drain accepted messages while channels can still deliver replies."""
        self._running = False
        # Reject new provider work first. Existing workers keep draining during
        # manager.stop(), and channel transports remain alive until that drain
        # completes so an already-sent "Working on it..." can still receive its
        # final update.
        await self.manager.stop()
        stop_errors: list[Exception] = []
        if self.inbound_receipt_processor is not None:
            try:
                await self.inbound_receipt_processor.stop()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Error stopping inbound receipt processor")
                stop_errors.append(exc)
        for name, channel in list(self._channels.items()):
            try:
                await channel.stop()
            except asyncio.CancelledError:
                # Keep this and the remaining transports owned by the service.
                # The Gateway deadline interrupted shutdown, so detaching them
                # would hide resources that may still be in use.
                raise
            except Exception as exc:
                logger.exception("Error stopping channel")
                stop_errors.append(exc)
            else:
                if self._channels.get(name) is channel:
                    self._channels.pop(name, None)
                logger.info("Channel stopped")

        if stop_errors:
            raise ExceptionGroup("one or more channels failed to stop", stop_errors)
        logger.info("ChannelService stopped")

    @property
    def github_ingress_durability(self) -> str:
        """Return the truthful signed-GitHub receipt support level."""

        if self.inbound_receipt_processor is not None and self.inbound_receipt_processor.durable:
            return "durable"
        return "best_effort"

    async def accept_verified_inbound_batch(
        self,
        messages: Sequence[Any],
    ) -> None:
        """Persist a verified fan-out before acknowledgment, or label it local-only."""

        batch = tuple(messages)
        if not batch:
            return
        processor = self.inbound_receipt_processor
        if processor is not None:
            if self._durable_production and not getattr(processor, "durable", False):
                raise RuntimeError("durable inbound receipt processor is unavailable")
            await processor.receive_batch(batch)
            return
        if self._durable_production:
            raise RuntimeError("durable inbound receipt processor is unavailable")
        for message in batch:
            await self.bus.publish_inbound(message)

    def _load_channel_config(self, name: str) -> dict[str, Any] | None:
        """Load the latest config for a specific channel from disk.

        Uses ``get_app_config()`` which detects file changes via config
        signature, so edits to ``config.yaml`` are picked up without a process
        restart.
        The UI runtime-config overlay applied at startup is re-applied here
        so a file-driven reload neither drops credentials entered from the
        browser nor resurrects a channel disconnected from it.
        Falls back to the cached ``self._config`` when config loading fails.
        """
        try:
            from deerflow.config.app_config import get_app_config

            app_config = get_app_config()
            extra = app_config.model_extra or {}
            channels_config = dict(extra.get("channels") or {})
            _merge_channel_connection_runtime_config(channels_config, app_config)
            channel_config = channels_config.get(name)
            if isinstance(channel_config, dict):
                # Update the cached config so get_status() stays consistent.
                self._config[name] = channel_config
                return channel_config
        except Exception:
            logger.exception("Failed to reload config for channel %s, using cached version", name)
        return self._config.get(name)

    async def restart_channel(self, name: str, *, reload_config: bool = True) -> bool:
        """Restart a specific channel. Returns True if successful."""
        if name in self._channels:
            try:
                await self._channels[name].stop()
            except Exception:
                logger.exception("Error stopping channel for restart")
            del self._channels[name]

        if reload_config:
            # Reading config.yaml and the runtime store is disk IO; keep it
            # off the event loop.
            config = await asyncio.to_thread(self._load_channel_config, name)
        else:
            config = self._config.get(name)
        if not config or not isinstance(config, dict):
            logger.warning("No config for requested channel")
            return False

        if not config.get("enabled", False):
            logger.info("Channel %s is disabled, skipping restart", name)
            return True

        return await self._start_channel(name, config)

    async def configure_channel(self, name: str, config: dict[str, Any]) -> bool:
        """Apply runtime config for a channel and restart it if the service is running."""
        self._config[name] = dict(config)
        if not self._running:
            return True
        # The caller just supplied the authoritative config (e.g. credentials
        # entered in the browser that are never written to config.yaml) — a
        # file reload here would clobber it with the stale on-disk entry.
        return await self.restart_channel(name, reload_config=False)

    async def remove_channel(self, name: str) -> bool:
        """Remove runtime config for a channel and stop it if currently running."""
        self._config.pop(name, None)
        channel = self._channels.pop(name, None)
        if channel is None:
            return True
        try:
            await channel.stop()
            logger.info("Channel stopped and removed")
            return True
        except Exception:
            logger.exception("Error stopping channel for removal")
            return False

    async def _start_channel(self, name: str, config: dict[str, Any]) -> bool:
        """Instantiate and start a single channel."""
        import_path = _CHANNEL_REGISTRY.get(name)
        if not import_path:
            logger.warning("Unknown channel type")
            return False

        try:
            from deerflow.reflection import resolve_class

            channel_cls = resolve_class(import_path, base_class=None)
        except Exception:
            logger.exception("Failed to import channel class")
            return False

        try:
            config = dict(config)
            config["channel_store"] = self.store
            if name == "buzz" and "seen_event_store_path" not in config:
                # Durable processed-event ids for the Buzz connector's replay
                # guard. Wired here (like channel_store) rather than defaulted
                # inside the connector so that directly constructed channels
                # (tests, tooling) stay free of filesystem side effects.
                from deerflow.config.paths import get_paths

                config["seen_event_store_path"] = str(Path(get_paths().base_dir) / "channels" / "buzz_seen_events.json")
            if self._connection_repo is not None:
                config["connection_repo"] = self._connection_repo
            channel = channel_cls(bus=self.bus, config=config)
            self._channels[name] = channel
            await channel.start()
            if not channel.is_running:
                self._channels.pop(name, None)
                logger.error("Channel did not enter a running state after start()")
                return False
            logger.info("Channel started")
            return True
        except Exception:
            self._channels.pop(name, None)
            logger.exception("Failed to start channel")
            return False

    def get_status(self) -> dict[str, Any]:
        """Return status information for all channels."""
        channels_status = {}
        for name in _CHANNEL_REGISTRY:
            config = self._config.get(name, {})
            enabled = isinstance(config, dict) and config.get("enabled", False)
            running = name in self._channels and self._channels[name].is_running
            channels_status[name] = {
                "enabled": enabled,
                "running": running,
            }
        return {
            "service_running": self._running,
            "channels": channels_status,
        }

    def get_channel(self, name: str) -> Channel | None:
        """Return a running channel instance by name when available."""
        return self._channels.get(name)

    def is_channel_enabled(self, name: str) -> bool:
        """Return whether ``channels.<name>.enabled`` is truthy in the live config.

        Tracks the runtime-authoritative ``_config`` dict, which
        :meth:`configure_channel` updates when the UI flips the
        enabled flag — so callers that read this between requests get
        the current effective setting without re-reading config.yaml.
        Used by the GitHub webhook router as a fan-out kill-switch:
        ``channels.github.enabled: false`` skips dispatch even though
        the webhook route itself remains mounted (which is governed by
        ``GITHUB_WEBHOOK_SECRET``, not this flag).
        """
        config = self._config.get(name)
        if not isinstance(config, dict):
            return False
        return bool(config.get("enabled", False))

    def get_channel_config(self, name: str) -> dict[str, Any] | None:
        """Return a shallow copy of the live ``channels.<name>`` block, or None.

        Mirrors :meth:`is_channel_enabled` in tracking the runtime-
        authoritative ``_config`` dict, so callers see the same effective
        configuration the manager sees — including any updates pushed via
        :meth:`configure_channel` from the UI. Returns ``None`` when no
        config exists for ``name`` (rather than an empty dict) so callers
        can distinguish "not configured" from "configured with defaults".
        The shallow copy keeps callers from accidentally mutating live
        config state.
        """
        config = self._config.get(name)
        if not isinstance(config, dict):
            return None
        return dict(config)


# -- singleton access -------------------------------------------------------

_channel_service: ChannelService | None = None


def get_channel_service() -> ChannelService | None:
    """Get the singleton ChannelService instance (if started)."""
    return _channel_service


async def start_channel_service(
    app_config: AppConfig | None = None,
    *,
    get_stream_bridge: Callable[[], StreamBridge | None] | None = None,
    invocation_runtime: InvocationRuntime | None = None,
) -> ChannelService:
    """Create and start the global ChannelService from app config.

    ``invocation_runtime`` and ``get_stream_bridge`` are threaded through
    ``ChannelService.from_app_config`` -> ``ChannelManager`` so channel launches
    share durable admission and fire_and_forget channels that opt into
    ``ChannelRunPolicy.buffer_followups_on_busy`` (currently GitHub) can watch
    a run's completion and auto-drain buffered follow-ups. ``app.py``'s
    lifespan passes a closure over ``app.state.stream_bridge`` and constructs
    the channel runtime beside the Scheduled Task runtime.
    """
    global _channel_service
    if _channel_service is not None:
        return _channel_service
    # from_app_config reads the JSON channel store and runtime config files;
    # keep that disk IO off the event loop. asyncio.to_thread forwards both
    # args and kwargs to the target callable.
    factory_kwargs: dict[str, Any] = {"get_stream_bridge": get_stream_bridge}
    if invocation_runtime is not None:
        factory_kwargs["invocation_runtime"] = invocation_runtime
    _channel_service = await asyncio.to_thread(
        ChannelService.from_app_config,
        app_config,
        **factory_kwargs,
    )
    await _channel_service.start()
    return _channel_service


async def stop_channel_service() -> None:
    """Stop the global ChannelService."""
    global _channel_service
    if _channel_service is not None:
        service = _channel_service
        await service.stop()
        if _channel_service is service:
            _channel_service = None
