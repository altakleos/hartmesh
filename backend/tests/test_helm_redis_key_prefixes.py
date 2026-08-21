"""Helm contracts for tenant-scoped Redis key prefixes."""

from support.helm import deployment_env

_PREFIX_ENVS = {
    "DEER_FLOW_STREAM_BRIDGE_KEY_PREFIX": "tenant",
    "DEER_FLOW_CHECKPOINT_CACHE_KEY_PREFIX": "tenant",
    "DEER_FLOW_SANDBOX_OWNERSHIP_KEY_PREFIX": "tenant",
}


def test_redis_key_prefix_envs_are_omitted_by_default() -> None:
    environment = deployment_env("gateway")

    assert _PREFIX_ENVS.keys().isdisjoint(environment)


def test_redis_key_prefix_values_render_on_gateway() -> None:
    environment = deployment_env(
        "gateway",
        "--set-string",
        "redis.keyPrefixes.streamBridge=tenant",
        "--set-string",
        "redis.keyPrefixes.checkpointCache=tenant",
        "--set-string",
        "redis.keyPrefixes.sandboxOwnership=tenant",
    )

    assert {name: environment[name] for name in _PREFIX_ENVS} == _PREFIX_ENVS
