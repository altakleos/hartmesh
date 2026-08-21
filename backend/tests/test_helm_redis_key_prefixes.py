"""Helm contracts for tenant-scoped Redis key prefixes."""

from support.helm import deployment_env

_PREFIX_ENVS = {
    "DEER_FLOW_STREAM_BRIDGE_KEY_PREFIX": "acme",
    "DEER_FLOW_CHECKPOINT_CACHE_KEY_PREFIX": "acme:ckpt-hist:v1",
    "DEER_FLOW_SANDBOX_OWNERSHIP_KEY_PREFIX": "acme:deerflow:sandbox:owner",
}


def test_redis_key_prefix_envs_are_omitted_by_default() -> None:
    environment = deployment_env("gateway")

    assert _PREFIX_ENVS.keys().isdisjoint(environment)


def test_tenant_prefix_derives_subsystem_namespaces() -> None:
    environment = deployment_env(
        "gateway",
        "--set-string",
        "redis.tenantPrefix=acme",
    )

    assert {name: environment[name] for name in _PREFIX_ENVS} == _PREFIX_ENVS


def test_explicit_subsystem_prefix_overrides_only_that_derived_value() -> None:
    environment = deployment_env(
        "gateway",
        "--set-string",
        "redis.tenantPrefix=acme",
        "--set-string",
        "redis.keyPrefixes.checkpointCache=cache-override",
    )

    assert environment["DEER_FLOW_STREAM_BRIDGE_KEY_PREFIX"] == "acme"
    assert environment["DEER_FLOW_CHECKPOINT_CACHE_KEY_PREFIX"] == "cache-override"
    assert environment["DEER_FLOW_SANDBOX_OWNERSHIP_KEY_PREFIX"] == "acme:deerflow:sandbox:owner"
