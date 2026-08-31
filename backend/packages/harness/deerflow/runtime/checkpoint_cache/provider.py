"""Cache factory. Mirrors make_stream_bridge: config -> env fallback -> memory."""

from __future__ import annotations

import contextlib
import hashlib
import logging
import os
from collections.abc import AsyncIterator
from typing import Any

from deerflow.config.app_config import AppConfig
from deerflow.runtime.checkpoint_cache.base import CACHE_FORMAT_VERSION, CheckpointHistoryCache
from deerflow.runtime.checkpoint_cache.memory import MemoryCheckpointHistoryCache
from deerflow.runtime.tenant_identity import (
    LegacyRedisPrefixRecordV1,
    RedisTenantComponent,
    TenantNamespaceV1,
    redis_component_key_prefix,
)

logger = logging.getLogger(__name__)

_ENV_REDIS_URL = "DEER_FLOW_CHECKPOINT_CACHE_REDIS_URL"
_ENV_KEY_PREFIX = "DEER_FLOW_CHECKPOINT_CACHE_KEY_PREFIX"


def _resolve_redis_url(config: Any) -> str:
    return config.redis_url or os.getenv(_ENV_REDIS_URL) or os.getenv("REDIS_URL") or "redis://localhost:6379/0"


def _stable_postgres_identity(postgres_url: str) -> str:
    """Credential-free database identity: host/port/database.

    Hashing the raw URL would change the cache namespace on every credential
    rotation (cold cache + orphaned keys until TTL) even though the database
    — and thus every cached checkpoint history — is unchanged. Unparseable
    URLs fall back to the raw string (still stable per deployment).
    """
    if not postgres_url:
        return ""
    try:
        from sqlalchemy.engine.url import make_url

        parsed = make_url(postgres_url)
    except Exception:  # noqa: BLE001 - identity must never fail config load
        return postgres_url
    return f"{parsed.host or 'localhost'}:{parsed.port or 5432}/{parsed.database or ''}"


def checkpoint_cache_db_hash(db_config: Any) -> str:
    """Deployment-identity hash so two deployments sharing one Redis never collide."""
    backend = getattr(db_config, "backend", "memory")
    if backend == "postgres":
        identity = f"postgres:{_stable_postgres_identity(getattr(db_config, 'postgres_url', ''))}:{getattr(db_config, 'postgres_schema', '')}"
    elif backend == "sqlite":
        identity = f"sqlite:{getattr(db_config, 'checkpointer_sqlite_path', '')}"
    else:
        identity = "memory"
    return hashlib.sha256(identity.encode()).hexdigest()[:12]


def checkpoint_cache_key_prefix(
    app_config: AppConfig,
    tenant_namespace: TenantNamespaceV1 | None = None,
    legacy_redis_prefixes: LegacyRedisPrefixRecordV1 | None = None,
) -> str:
    env_prefix = os.getenv(_ENV_KEY_PREFIX)
    cache_config = app_config.database.checkpoint_cache
    configured = env_prefix if env_prefix is not None else cache_config.key_prefix
    if tenant_namespace is not None:
        field = _ENV_KEY_PREFIX if env_prefix is not None else "database.checkpoint_cache.key_prefix"
        return redis_component_key_prefix(
            tenant_namespace,
            RedisTenantComponent.CHECKPOINT_CACHE,
            configured_prefix=configured or None,
            configured_field=field,
            legacy_record=legacy_redis_prefixes,
        )
    if configured:
        return configured
    return f"ckpt-hist:v{CACHE_FORMAT_VERSION}:{checkpoint_cache_db_hash(app_config.database)}"


@contextlib.asynccontextmanager
async def make_checkpoint_cache(
    app_config: AppConfig | None = None,
    *,
    serde: Any,
    tenant_namespace: TenantNamespaceV1 | None = None,
    legacy_redis_prefixes: LegacyRedisPrefixRecordV1 | None = None,
) -> AsyncIterator[CheckpointHistoryCache]:
    """Yield a history cache for the caller's lifetime.

    ``max_entries == 0`` disables the cache uniformly (both types) via a
    disabled memory backend, so the wrapper never needs a None check.
    """
    config = app_config.database.checkpoint_cache if app_config is not None else None

    if config is None or config.type == "memory" or config.max_entries == 0:
        max_entries = config.max_entries if config is not None else 128
        cache = MemoryCheckpointHistoryCache(max_entries=max_entries)
        logger.info("Checkpoint history cache initialised: memory (max_entries=%d)", max_entries)
        try:
            yield cache
        finally:
            await cache.aclose()
        return

    if config.type == "redis":
        from deerflow.runtime.checkpoint_cache.redis import RedisCheckpointHistoryCache

        # Validate any compatibility prefix even though the wrapper owns the
        # actual key construction.
        checkpoint_cache_key_prefix(
            app_config,
            tenant_namespace,
            legacy_redis_prefixes,
        )

        cache = RedisCheckpointHistoryCache(
            _resolve_redis_url(config),
            serde=serde,
            ttl_seconds=config.ttl_seconds,
        )
        logger.info("Checkpoint history cache initialised: redis (ttl_seconds=%d)", config.ttl_seconds)
        try:
            yield cache
        finally:
            await cache.aclose()
        return

    raise ValueError(f"Unknown checkpoint cache type: {config.type!r}")
