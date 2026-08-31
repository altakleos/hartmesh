"""Honcho memory backend — user-model memory via a Honcho (v3) instance.

Positioning (upstream RFC #1898): Honcho covers the user-dimension of memory —
long-term user modeling, preferences, cross-session working representation —
complementing project/task-oriented backends. Ingestion is cheap (plain message
writes); Honcho's own server-side deriver performs fact extraction and
representation building asynchronously, so this backend makes **no LLM calls**.

Multi-user isolation is owned by ``HonchoIdentityResolver``. In Gateway use,
the host supplies a validated tenant-derived namespace and the resolver adds a
readable user component plus a 16-hex SHA-256 suffix. Production overrides
must resolve to that same user workspace; only explicit local-development
configuration may share one. Missing users fail closed with no provider call.

Portability golden rule: the only ``from deerflow`` import is the contract line
below. Everything else arrives via ``backend_config``.
"""

from __future__ import annotations

import asyncio
import html
import logging
import threading
from typing import Any, ClassVar, Literal

from pydantic import PrivateAttr

# ABC contract -- the ONE allowed `from deerflow` import in this backend folder.
from deerflow.agents.memory.manager import MemoryManager, MemoryManagerError

from .client import HonchoClient
from .config import HonchoConfig, HonchoIdentityResolver, stable_id

logger = logging.getLogger(__name__)

_UTC_NOW_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
_MAX_SEARCH_RESULTS = 100
_MAX_WRITE_MESSAGES = 100


def _now_iso() -> str:
    import datetime

    return datetime.datetime.now(datetime.UTC).strftime(_UTC_NOW_FORMAT)


def _content_to_text(content: Any) -> str:
    """Normalize LangChain message content (str or content-block list) to text."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and isinstance(block.get("text"), str):
                parts.append(block["text"])
        return "\n".join(parts)
    return str(content or "")


def _stable_id(raw: str) -> str:
    """Readable-but-collision-resistant id for the default (non-override) path.

    The portable resolver reserves a 16-hex SHA-256 suffix before truncating
    the readable segment, so sanitization collisions remain disjoint.
    """
    return stable_id(raw)


class HonchoMemoryManager(MemoryManager):
    """MemoryManager backed by a Honcho v3 instance (self-hosted or hosted)."""

    _config: HonchoConfig = PrivateAttr(default=None)
    _identity: HonchoIdentityResolver = PrivateAttr(default=None)
    _client: Any = PrivateAttr(default=None)
    _memory_observer: Any = PrivateAttr(default=None)
    _health_lock: Any = PrivateAttr(default_factory=threading.Lock)
    _last_successful_probe_at: str | None = PrivateAttr(default=None)
    _last_failed_probe_at: str | None = PrivateAttr(default=None)
    _last_error_code: str | None = PrivateAttr(default=None)

    supports_search: ClassVar[bool] = True
    # Honcho's server-side deriver extracts facts/representation from add()
    # writes asynchronously; this backend implements no fact CRUD hooks
    # (create_fact/delete_fact/update_fact are unsupported), so tool mode
    # must retain passive writes (MemoryMiddleware -> add()) to keep feeding
    # the deriver, while search() supplies the query-aware retrieval tool
    # mode expects. Mirrors mem0_manager.py's identical rationale.
    requires_passive_writes_in_tool_mode: ClassVar[bool] = True

    def model_post_init(self, __context: Any) -> None:
        self._config = HonchoConfig.from_backend_config(self.backend_config)
        self._identity = HonchoIdentityResolver(self._config)
        self._client = HonchoClient(self._config)

    @classmethod
    def from_config(
        cls,
        backend_config: dict[str, Any] | None = None,
        *,
        mode: Literal["middleware", "tool"] = "middleware",
        **host_hooks: Any,
    ) -> HonchoMemoryManager:
        """Config errors (bad URL/insecure key) raise here — fail fast at startup.

        Connectivity is deliberately NOT probed: a temporarily unreachable Honcho
        must not block Gateway startup; reads degrade per ``failure_policy.read``.
        """
        manager = cls(backend_config=backend_config, mode=mode)
        manager._memory_observer = host_hooks.get("memory_observer")
        return manager

    # ── identity resolution (fail closed) ────────────────────────────────
    def _workspace(self, user_id: str | None) -> str | None:
        return self._identity.workspace(user_id)

    def _user_peer(self, user_id: str) -> str:
        return self._identity.user_peer(user_id)

    def _record_probe_success(self) -> None:
        with self._health_lock:
            self._last_successful_probe_at = _now_iso()
            self._last_error_code = None

    def _record_probe_failure(self, code: str) -> None:
        with self._health_lock:
            self._last_failed_probe_at = _now_iso()
            self._last_error_code = code

    def _observe(
        self,
        *,
        workspace: str,
        operation: str,
        status: str,
        safe_projection: Any | None,
        item_count: int | None,
        truncated: bool,
    ) -> None:
        observer = self._memory_observer
        if not callable(observer):
            return
        observer(
            workspace=workspace,
            operation=operation,
            status=status,
            safe_projection=safe_projection,
            item_count=item_count,
            truncated=truncated,
        )

    def _observe_read(
        self,
        *,
        workspace: str,
        operation: str,
        status: str,
        safe_projection: Any | None,
        item_count: int | None,
        truncated: bool,
    ) -> bool:
        """Record a read observation, applying the configured failure policy."""

        try:
            self._observe(
                workspace=workspace,
                operation=operation,
                status=status,
                safe_projection=safe_projection,
                item_count=item_count,
                truncated=truncated,
            )
            return True
        except Exception as exc:
            logger.warning(
                "honcho memory observation append failed code=honcho_memory_recall_failed error_class=%s",
                type(exc).__name__,
            )
            if self._config.read_fail_closed:
                raise MemoryManagerError("honcho_memory_recall_failed") from None
            return False

    def _provider_read(self, *, workspace: str, operation: str, fn: Any) -> tuple[bool, Any]:
        """One secret-safe provider boundary for every recall path."""

        try:
            value = fn()
            self._record_probe_success()
            return True, value
        except Exception as exc:
            self._record_probe_failure("honcho_memory_recall_failed")
            status = "failed_closed" if self._config.read_fail_closed else "failed_open"
            try:
                self._observe(
                    workspace=workspace,
                    operation=operation,
                    status=status,
                    safe_projection=None,
                    item_count=None,
                    truncated=False,
                )
            except Exception as observation_exc:
                logger.warning(
                    "honcho memory failure observation append failed code=honcho_memory_recall_failed error_class=%s",
                    type(observation_exc).__name__,
                )
            if self._config.read_fail_closed:
                raise MemoryManagerError("honcho_memory_recall_failed") from None
            logger.warning(
                "honcho memory recall failed code=honcho_memory_recall_failed policy=fail_open error_class=%s",
                type(exc).__name__,
            )
            return False, None

    @staticmethod
    def _safe_text(value: Any, *, max_chars: int) -> tuple[str, bool]:
        escaped = html.escape(str(value or "").strip(), quote=False)
        return escaped[:max_chars], len(escaped) > max_chars

    def _safe_search_results(
        self,
        results: Any,
        *,
        top_k: int,
        category: str | None,
    ) -> tuple[list[dict[str, Any]], bool]:
        raw_items = list(results) if isinstance(results, list) else []
        limit = max(0, top_k)
        truncated = len(raw_items) > limit
        remaining = self._config.max_injection_chars
        projected: list[dict[str, Any]] = []
        for item in raw_items[:limit]:
            if remaining <= 0:
                truncated = True
                break
            if not isinstance(item, dict):
                truncated = True
                continue
            safe_content, content_truncated = self._safe_text(
                item.get("content", ""),
                max_chars=remaining,
            )
            if not safe_content:
                continue
            remaining -= len(safe_content)
            projected.append(
                {
                    "content": safe_content,
                    "category": html.escape(str(category or "memory"), quote=False)[:128],
                    "session_id": html.escape(str(item.get("session_id")), quote=False)[:256] if item.get("session_id") is not None else None,
                    "peer_id": html.escape(str(item.get("peer_id")), quote=False)[:256] if item.get("peer_id") is not None else None,
                    "created_at": html.escape(str(item.get("created_at")), quote=False)[:64] if item.get("created_at") is not None else None,
                }
            )
            truncated = truncated or content_truncated
            if remaining <= 0:
                truncated = truncated or len(projected) < min(len(raw_items), limit)
                break
        return projected, truncated

    # ── Tier 1: write ────────────────────────────────────────────────────
    def add(
        self,
        thread_id: str,
        messages: list[Any],
        *,
        agent_name: str | None = None,
        user_id: str | None = None,
        trace_id: str | None = None,
    ) -> None:
        workspace = self._workspace(user_id)
        if workspace is None or not user_id:
            logger.debug("honcho memory: no resolvable user; skipping write")
            return
        user_peer = self._user_peer(user_id)
        assistant_peer = self._identity.assistant_peer()
        outgoing: list[dict[str, str]] = []
        truncated = False
        for message in messages or []:
            msg_type = getattr(message, "type", None)
            text = _content_to_text(getattr(message, "content", "")).strip()
            if not text:
                continue
            truncated = truncated or len(text) > self._config.message_char_limit
            if msg_type == "human":
                outgoing.append({"peer_id": user_peer, "content": text[: self._config.message_char_limit]})
            elif msg_type in ("ai", "AIMessageChunk"):
                outgoing.append({"peer_id": assistant_peer, "content": text[: self._config.message_char_limit]})
        if not outgoing:
            return
        if len(outgoing) > _MAX_WRITE_MESSAGES:
            outgoing = outgoing[-_MAX_WRITE_MESSAGES:]
            truncated = True
        session_id = self._identity.session(thread_id)
        try:
            self._client.get_or_create_peer(workspace, user_peer)
            self._client.get_or_create_peer(workspace, assistant_peer)
            self._client.get_or_create_session(workspace, session_id)
            self._client.set_session_peers(workspace, session_id, [user_peer, assistant_peer])
            self._client.add_messages(workspace, session_id, outgoing)
            self._record_probe_success()
            try:
                self._observe(
                    workspace=workspace,
                    operation="add",
                    status="succeeded",
                    # Writes are not projected into model context. Record
                    # bounded status/count evidence without hashing raw
                    # conversation content as though it were an injection.
                    safe_projection=None,
                    item_count=len(outgoing),
                    truncated=truncated,
                )
            except Exception as exc:
                logger.warning(
                    "honcho memory write observation append failed code=honcho_memory_recall_failed error_class=%s",
                    type(exc).__name__,
                )
        except Exception as exc:
            self._record_probe_failure("honcho_request_failed")
            try:
                self._observe(
                    workspace=workspace,
                    operation="add",
                    status="failed_open",
                    safe_projection=None,
                    item_count=None,
                    truncated=truncated,
                )
            except Exception:
                pass
            logger.warning(
                "honcho memory write failed code=honcho_request_failed error_class=%s",
                type(exc).__name__,
            )

    # ── Tier 1: read ─────────────────────────────────────────────────────
    def get_context(
        self,
        user_id: str | None,
        *,
        agent_name: str | None = None,
        thread_id: str | None = None,
    ) -> str:
        workspace = self._workspace(user_id)
        if workspace is None or not user_id:
            return ""
        succeeded, representation = self._provider_read(
            workspace=workspace,
            operation="get_context",
            fn=lambda: self._client.working_representation(workspace, self._user_peer(user_id), max_conclusions=25),
        )
        if not succeeded:
            return ""
        projected, truncated = self._safe_text(
            representation,
            max_chars=self._config.max_injection_chars,
        )
        status = "succeeded" if projected else "empty"
        if not self._observe_read(
            workspace=workspace,
            operation="get_context",
            status=status,
            safe_projection=projected or None,
            item_count=1 if projected else 0,
            truncated=truncated,
        ):
            return ""
        return projected

    # ── Tier 2 ───────────────────────────────────────────────────────────
    def search(
        self,
        query: str,
        top_k: int = 5,
        *,
        user_id: str | None = None,
        agent_name: str | None = None,
        category: str | None = None,
    ) -> list[dict[str, Any]]:
        workspace = self._workspace(user_id)
        if workspace is None:
            return []
        effective_top_k = max(0, min(top_k, _MAX_SEARCH_RESULTS))
        succeeded, results = self._provider_read(
            workspace=workspace,
            operation="search",
            fn=lambda: self._client.search(workspace, query, limit=effective_top_k),
        )
        if not succeeded:
            return []
        projected, truncated = self._safe_search_results(
            results,
            top_k=effective_top_k,
            category=category,
        )
        result_count = len(results) if isinstance(results, list) else 0
        truncated = truncated or (top_k > effective_top_k and result_count >= effective_top_k)
        if not self._observe_read(
            workspace=workspace,
            operation="search",
            status="succeeded" if projected else "empty",
            safe_projection=projected or None,
            item_count=len(projected),
            truncated=truncated,
        ):
            return []
        return projected

    def get_memory(
        self,
        *,
        user_id: str | None = None,
        agent_name: str | None = None,
    ) -> dict[str, Any]:
        """Minimal DeerMem-shape view: representation as the work-context summary.

        Honcho has no DeerMem-style fact CRUD; the gateway fills missing fields
        with defaults (same contract as the noop backend's ``{"facts": []}``).
        """
        empty = {"facts": [], "lastUpdated": _now_iso(), "user": {}, "history": {}}
        workspace = self._workspace(user_id)
        if workspace is None or not user_id:
            return empty
        succeeded, representation = self._provider_read(
            workspace=workspace,
            operation="get_memory",
            fn=lambda: self._client.working_representation(workspace, self._user_peer(user_id), max_conclusions=25),
        )
        if not succeeded:
            return empty
        projected, truncated = self._safe_text(
            representation,
            max_chars=self._config.max_injection_chars,
        )
        if not self._observe_read(
            workspace=workspace,
            operation="get_memory",
            status="succeeded" if projected else "empty",
            safe_projection=projected or None,
            item_count=1 if projected else 0,
            truncated=truncated,
        ):
            return empty
        now = _now_iso()
        return {
            "facts": [],
            "lastUpdated": now,
            "user": {"workContext": {"summary": projected, "updatedAt": now}},
            "history": {},
        }

    def shutdown_flush(self, timeout: float) -> bool:
        """Writes are synchronous per-call; nothing is buffered locally."""
        return True

    def close(self) -> None:
        """Release the HTTP client (gateway shutdown hook)."""
        self._client.close()

    def safe_diagnostics(self) -> dict[str, object]:
        """Return the bounded operator projection; never provider/user data."""

        with self._health_lock:
            last_successful = self._last_successful_probe_at
            last_failed = self._last_failed_probe_at
            last_error_code = self._last_error_code
        if self._config.base_url.lower().startswith("https://"):
            transport_security = "https"
        elif self._config.api_key:
            transport_security = "insecure_local_http"
        else:
            transport_security = "http_without_credentials"
        if last_error_code is not None:
            operational_status = "degraded"
        elif last_successful is not None:
            operational_status = "available"
        else:
            operational_status = "not_observed"
        return {
            **dict(self._identity.safe_diagnostics()),
            "backend": "honcho",
            "selected": True,
            "initialized": True,
            "dependency_role": "mutable_contextual_memory",
            "durable_dependency": False,
            "transport_security": transport_security,
            "read_failure_policy": "fail_closed" if self._config.read_fail_closed else "fail_open",
            "operational_status": operational_status,
            "last_successful_probe_at": last_successful,
            "last_failed_probe_at": last_failed,
            "last_error_code": last_error_code,
        }

    # ── async offload (blocking-io gate: never run httpx on the event loop) ──
    async def aadd(
        self,
        thread_id: str,
        messages: list[Any],
        *,
        agent_name: str | None = None,
        user_id: str | None = None,
        trace_id: str | None = None,
    ) -> None:
        await asyncio.to_thread(self.add, thread_id, messages, agent_name=agent_name, user_id=user_id, trace_id=trace_id)

    async def aget_context(
        self,
        user_id: str | None,
        *,
        agent_name: str | None = None,
        thread_id: str | None = None,
    ) -> str:
        return await asyncio.to_thread(self.get_context, user_id, agent_name=agent_name, thread_id=thread_id)

    async def asearch(
        self,
        query: str,
        top_k: int = 5,
        *,
        user_id: str | None = None,
        agent_name: str | None = None,
        category: str | None = None,
    ) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self.search, query, top_k, user_id=user_id, agent_name=agent_name, category=category)
