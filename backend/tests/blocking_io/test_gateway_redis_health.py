"""Configuration reads for public readiness stay off the event loop."""

import asyncio

import pytest


@pytest.mark.asyncio
async def test_redis_readiness_offloads_live_login_configuration(tmp_path):
    from app.gateway.redis_health import RequiredRedisReadiness
    from deerflow.config.auth_config import LocalAuthConfig
    from deerflow.runtime.stream_bridge.memory import MemoryStreamBridge

    path = tmp_path / "local-enabled"
    await asyncio.to_thread(path.write_text, "true", encoding="utf-8")

    def load_local():
        return LocalAuthConfig(enabled=path.read_text(encoding="utf-8") == "true")

    probe = RequiredRedisReadiness(MemoryStreamBridge(), local_config=load_local)
    try:
        assert await probe.check() == "not_configured"
    finally:
        await probe.close()
