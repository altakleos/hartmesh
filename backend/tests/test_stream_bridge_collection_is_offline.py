"""Importing bridge unit fixtures must not probe an operator's Redis service."""

import runpy
from pathlib import Path

import redis


def test_stream_bridge_collection_defers_redis_probe_to_integration_fixture(monkeypatch):
    probes = []

    class Probe:
        def ping(self):
            probes.append("ping")
            return True

        def close(self):
            pass

    monkeypatch.setattr(redis.Redis, "from_url", staticmethod(lambda *args, **kwargs: Probe()))
    namespace = runpy.run_path(str(Path(__file__).with_name("test_stream_bridge.py")))
    assert namespace["requires_redis"] is not None
    assert probes == [], "Ordinary fixture collection performed a Redis connectivity probe"
