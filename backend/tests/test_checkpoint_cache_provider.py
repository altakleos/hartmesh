"""Provider wiring: mode-gated wrapping in async and sync checkpointer factories."""

import pytest

from deerflow.config.app_config import AppConfig, set_app_config
from deerflow.runtime.capture_resources import OwnedCheckpointSaver
from deerflow.runtime.checkpoint_mode import freeze_checkpoint_channel_mode
from deerflow.runtime.checkpointer.async_provider import make_checkpointer
from deerflow.runtime.checkpointer.cached_saver import CachedHistorySaver
from deerflow.runtime.checkpointer.provider import checkpointer_context, reset_checkpointer


@pytest.fixture(autouse=True)
def composition(monkeypatch):
    """Observe real constructor inputs; production exposes no cache/raw handle."""
    from deerflow.runtime import capture_resources
    from deerflow.runtime.checkpoint_cache.memory import MemoryCheckpointHistoryCache

    observed = {"inners": {}, "caches": []}
    initialize = MemoryCheckpointHistoryCache.__init__

    def owned(inner, **kwargs):
        result = OwnedCheckpointSaver(inner, **kwargs)
        observed["inners"][id(result)] = inner
        return result

    def cache(instance, *args, **kwargs):
        initialize(instance, *args, **kwargs)
        observed["caches"].append(instance)

    monkeypatch.setattr(capture_resources, "OwnedCheckpointSaver", owned)
    monkeypatch.setattr(MemoryCheckpointHistoryCache, "__init__", cache)
    return observed


# AppConfig requires the sandbox section (no default); the rest of the config
# is optional. Mirrors test_checkpoint_cache_redis.py's construction pattern.
def _app_config(mode: str, cache: dict | None = None) -> AppConfig:
    database: dict = {"backend": "memory", "checkpoint_channel_mode": mode}
    if cache is not None:
        database["checkpoint_cache"] = cache
    return AppConfig.model_validate(
        {
            "sandbox": {"use": "deerflow.sandbox.local.provider:LocalSandboxProvider"},
            "database": database,
        }
    )


@pytest.mark.anyio
async def test_delta_mode_wraps_with_cached_saver(composition):
    set_app_config(_app_config("delta"))
    freeze_checkpoint_channel_mode("delta")
    async with make_checkpointer() as saver:
        assert isinstance(saver, OwnedCheckpointSaver)
        inner = composition["inners"][id(saver)]
        assert isinstance(inner, CachedHistorySaver)
        assert inner.stats()["entries"] == 0


@pytest.mark.anyio
async def test_full_mode_yields_raw_saver(composition):
    set_app_config(_app_config("full"))
    freeze_checkpoint_channel_mode("full")
    async with make_checkpointer() as saver:
        assert isinstance(saver, OwnedCheckpointSaver)
        assert not isinstance(composition["inners"][id(saver)], CachedHistorySaver)


@pytest.mark.anyio
async def test_zero_max_entries_disables_but_still_wraps(composition):
    set_app_config(_app_config("delta", {"max_entries": 0}))
    freeze_checkpoint_channel_mode("delta")
    async with make_checkpointer() as saver:
        assert isinstance(saver, OwnedCheckpointSaver)
        assert isinstance(composition["inners"][id(saver)], CachedHistorySaver)
        # Disabled cache -> every history call is a full walk on the inner saver.
        assert composition["caches"][-1].enabled is False


def test_sync_delta_mode_wraps_memory(composition):
    set_app_config(_app_config("delta"))
    freeze_checkpoint_channel_mode("delta")
    reset_checkpointer()
    with checkpointer_context() as saver:
        assert isinstance(saver, OwnedCheckpointSaver)
        assert isinstance(composition["inners"][id(saver)], CachedHistorySaver)


def test_sync_redis_cache_type_is_config_error():
    set_app_config(_app_config("delta", {"type": "redis"}))
    freeze_checkpoint_channel_mode("delta")
    reset_checkpointer()
    with pytest.raises(ValueError, match="redis"):
        with checkpointer_context():
            pass


def test_sync_full_mode_unwrapped(composition):
    set_app_config(_app_config("full"))
    freeze_checkpoint_channel_mode("full")
    reset_checkpointer()
    with checkpointer_context() as saver:
        assert isinstance(saver, OwnedCheckpointSaver)
        assert not isinstance(composition["inners"][id(saver)], CachedHistorySaver)


def test_sync_cache_recreated_when_key_prefix_changes(composition):
    """The singleton must not outlive its namespace: a prefix change without
    a process restart leaves old-prefix entries unreachable and unpurgeable."""
    reset_checkpointer()
    set_app_config(_app_config("delta", {"key_prefix": "ns-a"}))
    freeze_checkpoint_channel_mode("delta")
    with checkpointer_context() as saver:
        assert isinstance(saver, OwnedCheckpointSaver)
        first_cache = composition["caches"][-1]
        first_cache.set_many({"ns-a:t1:x": {"writes": []}})
    # Same prefix: singleton is reused (warm across wrappers).
    with checkpointer_context() as saver:
        assert isinstance(saver, OwnedCheckpointSaver)
        assert composition["caches"][-1] is first_cache
        assert first_cache.stats().entries == 1
    # Prefix change: fresh cache, stale namespace gone with the old instance.
    set_app_config(_app_config("delta", {"key_prefix": "ns-b"}))
    with checkpointer_context() as saver:
        assert isinstance(saver, OwnedCheckpointSaver)
        assert composition["caches"][-1] is not first_cache
        assert composition["caches"][-1].stats().entries == 0
    reset_checkpointer()
