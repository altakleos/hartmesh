"""Offline contracts for the tenant VM compose profile under deploy/compose.

The profile is a released deployment path beside the Helm chart: one KVM
guest per customer, the whole stack under Docker Compose, sandboxes created
by the Gateway's local Docker backend. These tests pin what the profile
promises without a Docker daemon: the .env contract, the published surface,
the memory budget, the config render, the nginx render, and the release
pinning that makes ``images.txt`` and the profile agree byte for byte.
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
from collections.abc import Iterator
from ipaddress import ip_network
from pathlib import Path
from types import ModuleType

import pytest
import yaml
from _compose_network_ranges import DOCKER_DEFAULT_POOLS, EXTERNAL_RANGES

REPO_ROOT = Path(__file__).resolve().parents[2]
PROFILE = REPO_ROOT / "deploy" / "compose"
COMPOSE = PROFILE / "compose.yaml"
TEMPLATE = PROFILE / "config.yaml"
CATALOG = PROFILE / "providers"
IMAGES = PROFILE / "images.txt"
RELEASE = "2.1.0+hartmesh.5"
RELEASE_IMAGE_TAG = "v2.1.0-hartmesh.5"
CONTRACT_KEYS = {
    "HARTMESH_TENANT",
    "HARTMESH_PUBLIC_HOST",
    "HARTMESH_TRUSTED_PROXIES",
    "HARTMESH_LISTEN",
    "HARTMESH_DATA_DIR",
    "SANDBOX_RUNTIME",
    "SANDBOX_EGRESS",
    "POSTGRES_PASSWORD",
    "REDIS_PASSWORD",
    "AUTH_JWT_SECRET",
}
SERVICES = {"gateway", "frontend", "nginx", "postgres", "redis"}
OPTIONAL_KEYS = {"HARTMESH_APP_SUBNET"}
MEMORY_MIB = {"gateway": 1344, "frontend": 384, "nginx": 128, "postgres": 768, "redis": 256}
NGINX_VARIABLES = {
    "$forwarded_proto",
    "$remote_addr",
    "$proxy_add_x_forwarded_for",
    "$http_host",
    "$gateway_upstream",
    "$http_upgrade",
    "$connection_upgrade",
    "$scheme",
    "$frontend_upstream",
    "$provisioner_upstream",
    "$http_x_forwarded_proto",
}
_BARE_VARIABLE = re.compile(r"\$[a-z_]+")
_ENV_REFERENCE = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)")
_APP_SUBNET = re.compile(r"\$\{HARTMESH_APP_SUBNET:-([^}]+)\}")


def _load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def render_config() -> Iterator[ModuleType]:
    module = _load_module("hartmesh_render_config_test", PROFILE / "gateway" / "render_config.py")
    try:
        yield module
    finally:
        sys.modules.pop("hartmesh_render_config_test", None)


@pytest.fixture(scope="module")
def pin_images() -> Iterator[ModuleType]:
    module = _load_module("hartmesh_pin_compose_images_test", REPO_ROOT / "scripts" / "pin_compose_images.py")
    try:
        yield module
    finally:
        sys.modules.pop("hartmesh_pin_compose_images_test", None)


@pytest.fixture(scope="module")
def compose() -> dict:
    return yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))


@pytest.fixture(autouse=True)
def _restore_config_singletons() -> Iterator[None]:
    """Undo the process-wide state ``AppConfig.from_file`` leaves behind.

    Loading a file applies it to singletons that ``reset_app_config()`` does
    not put back (``_apply_singleton_configs``). Two tests here load the tenant
    profile, which selects PostgreSQL, so without this the next test *in the
    same process* -- in this file or any other the shard happens to schedule
    after it -- builds a postgres checkpointer and fails resolving the host.
    Only the checkpointer singleton is restored: it is the one with reach
    outside this module. The leak itself belongs to ``from_file``, not here.
    """
    from deerflow.config.app_config import reset_app_config
    from deerflow.config.checkpointer_config import get_checkpointer_config, set_checkpointer_config

    previous = get_checkpointer_config()
    try:
        yield
    finally:
        set_checkpointer_config(previous)
        reset_app_config()


def _mib(value: str) -> int:
    assert value.endswith("m"), value
    return int(value[:-1])


def _base_environ() -> dict[str, str]:
    return {"DATABASE_URL": "postgresql://deerflow:x@postgres:5432/deerflow", "DEER_FLOW_STREAM_BRIDGE_REDIS_URL": "redis://:x@redis:6379/0"}


# ── compose.yaml ─────────────────────────────────────────────────────────────


def test_profile_declares_exactly_the_five_services_and_no_named_volumes(compose: dict) -> None:
    assert compose["name"] == "hartmesh"
    assert set(compose["services"]) == SERVICES
    assert "volumes" not in compose, "named volumes would live on the root disk, which is not tenant data"
    for name, service in compose["services"].items():
        assert service.get("restart") == "unless-stopped", name
        assert "logging" not in service, f"{name}: the daemon's journald default must not be overridden"


def test_only_nginx_publishes_a_port_and_the_contract_carries_the_bind(compose: dict) -> None:
    published = {name: service.get("ports") for name, service in compose["services"].items() if service.get("ports")}
    assert set(published) == {"nginx"}
    (mapping,) = published["nginx"]
    assert mapping.startswith("${HARTMESH_LISTEN") and mapping.endswith(":2026")
    assert compose["services"]["nginx"].get("user") == "101:101"


def test_memory_limits_sum_to_2880_mib_with_equal_swap(compose: dict) -> None:
    total = 0
    for name, expected in MEMORY_MIB.items():
        service = compose["services"][name]
        assert _mib(service["mem_limit"]) == expected, name
        assert service["memswap_limit"] == service["mem_limit"], name
        total += expected
    assert total == 2880, "3072 less the 192 MiB two 1 GiB sandboxes cost over 768 MiB (README: Memory budget)"


PIDS_LIMIT = {"gateway": 2048, "frontend": 512, "nginx": 256, "postgres": 512, "redis": 128}


def test_every_service_bounds_its_processes_and_the_app_tier_its_cpus(compose: dict) -> None:
    for name, expected in PIDS_LIMIT.items():
        assert compose["services"][name]["pids_limit"] == expected, name
    for name in ("gateway", "frontend"):
        assert compose["services"][name]["cpus"] == 2, f"{name} shares four vCPUs with two sandboxes at --cpus 1"
    for name in ("nginx", "postgres", "redis"):
        assert "cpus" not in compose["services"][name], name


def test_two_sandboxes_with_proxies_fit_the_five_gib_budget(compose: dict) -> None:
    env = compose["services"]["gateway"]["environment"]
    sandbox = _mib(env["DEER_FLOW_SANDBOX_MEMORY"])
    proxy = _mib(env["DEER_FLOW_SANDBOX_PROXY_MEMORY"])
    services = sum(MEMORY_MIB.values())
    assert sandbox == 1024, "the only value that held in every provider-driven gVisor run (README: Memory budget)"
    assert services + 2 * (sandbox + proxy) == 5120, "exactly on the 5.0 GiB line: raising any limit in compose.yaml must be paid for by lowering another"
    assert 5120 < services + 3 * (sandbox + proxy)
    assert services + 2 * sandbox <= 5120 < services + 3 * sandbox, "open mode carries no proxy, fits two and not three"


def test_only_the_gateway_reads_the_env_file_and_the_others_get_explicit_environment(compose: dict) -> None:
    services = compose["services"]
    with_env_file = {name for name, service in services.items() if service.get("env_file")}
    assert with_env_file == {"gateway"}
    (env_file,) = services["gateway"]["env_file"]
    assert env_file == {"path": "${HARTMESH_DATA_DIR}/.env", "required": False}
    assert set(services["frontend"]["environment"]) == {"NODE_ENV", "DEER_FLOW_INTERNAL_GATEWAY_BASE_URL"}
    assert services["frontend"]["environment"]["DEER_FLOW_INTERNAL_GATEWAY_BASE_URL"] == "http://gateway:8001"
    assert set(services["nginx"]["environment"]) == {"HARTMESH_PUBLIC_HOST", "HARTMESH_TRUSTED_PROXIES"}
    for name in ("frontend", "nginx", "postgres", "redis"):
        assert "AUTH_JWT_SECRET" not in yaml.safe_dump(services[name]), name


def test_bind_mounts_stay_under_the_data_directory_or_the_read_only_bundle(compose: dict) -> None:
    for name, service in compose["services"].items():
        for volume in service.get("volumes", []):
            if isinstance(volume, dict):
                assert volume["type"] == "tmpfs", (name, volume)
                continue
            source, _, rest = volume.partition(":")
            if source == "/var/run/docker.sock":
                assert name == "gateway"
                continue
            if source.startswith("./"):
                assert rest.endswith(":ro") and rest.startswith("/opt/hartmesh/"), (name, volume)
                continue
            assert source.startswith("${HARTMESH_DATA_DIR"), (name, volume)


def test_gateway_wiring_follows_the_contract(compose: dict) -> None:
    gateway = compose["services"]["gateway"]
    env = gateway["environment"]
    assert "user" not in gateway, "the entrypoint drops privileges itself after reading the socket's group"
    assert gateway["command"] == ["sh", "/opt/hartmesh/gateway/entrypoint.sh"]
    assert env["DEER_FLOW_TENANT_ID"].startswith("${HARTMESH_TENANT")
    assert env["DEER_FLOW_SANDBOX_RUNTIME"].startswith("${SANDBOX_RUNTIME")
    assert env["DEER_FLOW_HOME"] == "${HARTMESH_DATA_DIR}/home"
    assert env["DEER_FLOW_HOST_BASE_DIR"] == env["DEER_FLOW_HOME"]
    assert env["DEER_FLOW_CONFIG_PATH"] == "${HARTMESH_DATA_DIR}/home/config.yaml"
    assert env["DEER_FLOW_EXTENSIONS_CONFIG_PATH"] == "${HARTMESH_DATA_DIR}/home/extensions_config.json"
    assert "${HARTMESH_DATA_DIR}/home:${HARTMESH_DATA_DIR}/home" in gateway["volumes"], "the data dir must be mounted at its host path"
    assert env["DATABASE_URL"] == "postgresql://deerflow:${POSTGRES_PASSWORD}@postgres:5432/deerflow"
    assert env["DEER_FLOW_STREAM_BRIDGE_REDIS_URL"] == "redis://:${REDIS_PASSWORD}@redis:6379/0"
    assert env["DEER_FLOW_SANDBOX_HOST"] == "host.docker.internal"
    assert "host.docker.internal:host-gateway" in gateway["extra_hosts"]
    assert env["DEER_FLOW_SANDBOX_NETWORK"] == "hartmesh_sandbox"
    assert env["DEER_FLOW_SANDBOX_MEMORY"] == "1024m"
    assert env["DEER_FLOW_SANDBOX_CPUS"] == "1"
    assert int(env["DEER_FLOW_SANDBOX_PIDS_LIMIT"]) > 0
    assert env["DEER_FLOW_SANDBOX_PROXY_MEMORY"].endswith("m")
    assert env["DEER_FLOW_SANDBOX_CONTAINER_USER"] == "1000:1000"
    assert env["DEER_FLOW_SANDBOX_IMAGE_STARTUP_CAPS"] == "0"
    assert env["DEER_FLOW_SANDBOX_SECCOMP_UNCONFINED"] == "0"
    assert "DEER_FLOW_SANDBOX_SECCOMP_PROFILE" not in env
    assert "DEER_FLOW_INTERNAL_AUTH_TOKEN" not in env, "one worker keeps the per-process token coherent without a second key"
    assert "BETTER_AUTH_SECRET" not in COMPOSE.read_text(encoding="utf-8")
    subnet = compose["networks"]["app"]["ipam"]["config"][0]["subnet"]
    assert env["AUTH_TRUSTED_PROXIES"] == subnet
    healthcheck = compose["services"]["gateway"]["healthcheck"]["test"]
    assert healthcheck[-1].count("/health/ready") == 1
    assert healthcheck[:6] == ["CMD", "setpriv", "--reuid=1000", "--regid=1000", "--clear-groups", "--no-new-privs"], "docker runs the probe as root; it must drop like the entrypoint"
    assert compose["services"]["gateway"]["cap_drop"] == ["ALL"]
    assert compose["services"]["gateway"]["cap_add"] == ["SETUID", "SETGID"], "the root window needs exactly the two capabilities the drop uses"


def test_sandbox_network_is_declared_and_joined_by_no_service(compose: dict) -> None:
    assert set(compose["networks"]) == {"app", "sandbox"}
    for name, service in compose["services"].items():
        assert service["networks"] == ["app"], name
    run = (PROFILE / "gateway" / "run.sh").read_text(encoding="utf-8")
    assert 'docker network create --driver bridge -o com.docker.network.bridge.enable_icc=false "$DEER_FLOW_SANDBOX_NETWORK"' in run, "under open, peers must not reach each other on the bridge"
    assert 'docker network inspect "$DEER_FLOW_SANDBOX_NETWORK"' in run


# ── the app bridge's address space ───────────────────────────────────────────


def _app_subnet_defaults() -> list[str]:
    return _APP_SUBNET.findall(COMPOSE.read_text(encoding="utf-8"))


def test_the_app_subnet_default_clears_every_recorded_external_range() -> None:
    """The defect this pins: until 2026-09-08 the app bridge was 172.30.10.0/24,
    which is inside the operator's kosmos pod range, and a bridge is a
    connected route in the guest. A public GET still working does not disprove
    the overlap -- the Kubernetes worker was SNAT'ing the request onto another
    leg -- so the check has to be on the address space, not on reachability."""
    defaults = set(_app_subnet_defaults())
    assert len(defaults) == 1, defaults
    subnet = ip_network(defaults.pop())
    assert subnet.version == 4 and subnet.is_private
    assert subnet.num_addresses >= 16, "the five services plus the bridge address must fit"
    for entry in EXTERNAL_RANGES:
        assert not subnet.overlaps(ip_network(entry)), f"the app bridge would swallow the route to {entry}"
    for entry in DOCKER_DEFAULT_POOLS:
        assert not subnet.overlaps(ip_network(entry)), f"pinning inside {entry} collides with, or spends, the pool the sandbox networks draw from"


def test_gateway_trust_and_the_app_network_are_the_same_string(compose: dict) -> None:
    """Moving IPAM alone would leave the Gateway trusting an address nginx no
    longer has: X-Real-IP would be ignored and the per-source spray guard would
    collapse onto the proxy's own address for the whole world. One override with
    one default at both sites makes that divergence unrepresentable, including
    when an operator sets the override."""
    references = _app_subnet_defaults()
    assert len(references) == 2 and len(set(references)) == 1, references
    subnet = compose["networks"]["app"]["ipam"]["config"][0]["subnet"]
    assert subnet == "${HARTMESH_APP_SUBNET:-" + references[0] + "}"
    assert compose["services"]["gateway"]["environment"]["AUTH_TRUSTED_PROXIES"] == subnet
    # The operator-facing key is nginx's trust of the front door and is untouched.
    assert compose["services"]["nginx"]["environment"]["HARTMESH_TRUSTED_PROXIES"] == "${HARTMESH_TRUSTED_PROXIES:?HARTMESH_TRUSTED_PROXIES is a required .env key}"


def test_the_login_path_honours_a_forwarded_address_only_from_the_app_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """The other half of that contract, on the Gateway's side: the shipped
    subnet is what makes nginx's X-Real-IP count, and a peer outside it cannot
    forge one."""
    from starlette.requests import Request

    from app.gateway.routers.auth import _get_client_ip

    subnet = ip_network(_app_subnet_defaults()[0])
    monkeypatch.setenv("AUTH_TRUSTED_PROXIES", str(subnet))
    hosts = list(subnet.hosts())
    proxy, outsider, client = str(hosts[1]), "203.0.113.7", "198.51.100.44"

    def _request(peer: str) -> Request:
        return Request({"type": "http", "method": "POST", "path": "/api/v1/auth/login/local", "headers": [(b"x-real-ip", client.encode())], "query_string": b"", "client": (peer, 51000), "server": ("testserver", 80)})

    assert _get_client_ip(_request(proxy)) == client, "nginx sits on the app network; its forwarded address is the one the spray guard counts"
    assert _get_client_ip(_request(outsider)) == outsider, "a peer off the app network cannot name its own source"
    # And the old subnet is no longer trusted, which is what makes the move real.
    assert _get_client_ip(_request("172.30.10.5")) == "172.30.10.5"


def test_datastores_run_as_the_data_directory_owner_with_relaxed_durability(compose: dict) -> None:
    postgres = compose["services"]["postgres"]
    redis = compose["services"]["redis"]
    assert postgres["user"] == "1000:1000" and redis["user"] == "1000:1000"
    assert postgres["command"] == ["postgres", "-c", "synchronous_commit=off", "-c", "wal_writer_delay=200ms"]
    assert postgres["stop_grace_period"] == "60s"
    assert "--appendfsync everysec" in redis["command"][-1]
    assert "--maxmemory 128mb --maxmemory-policy volatile-lru" in redis["command"][-1], "maxmemory is half the cgroup so an AOF rewrite fork fits; only TTL keys are evictable"
    assert "$$REDIS_PASSWORD" in redis["command"][-1]
    assert compose["services"]["gateway"]["depends_on"] == {"postgres": {"condition": "service_healthy"}, "redis": {"condition": "service_healthy"}}


# ── the .env contract ────────────────────────────────────────────────────────


def _compose_config(env_file: Path) -> subprocess.CompletedProcess[str]:
    if shutil.which("docker") is None:
        if os.environ.get("CI"):
            pytest.fail("docker compose is required in CI to verify the .env value alphabet")
        pytest.skip("docker is not installed")
    return subprocess.run(
        ["docker", "compose", "--project-directory", str(PROFILE), "--env-file", str(env_file), "config", "--format", "json"],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def test_single_quoted_env_values_pass_through_compose_verbatim(tmp_path: Path) -> None:
    """Compose's dotenv parser interpolates `$` and strips ` #` in bare values; single quotes carry them."""
    lines = (PROFILE / ".env.example").read_text(encoding="utf-8").splitlines()
    replaced = {"POSTGRES_PASSWORD": "pa$sword#x", "REDIS_PASSWORD": "ab${HOME}cd"}
    env_file = tmp_path / "tenant.env"
    env_file.write_text("".join(f"{line.split('=', 1)[0]}='{replaced[line.split('=', 1)[0]]}'\n" if line.split("=", 1)[0] in replaced else f"{line}\n" for line in lines), encoding="utf-8")
    result = _compose_config(env_file)
    assert result.returncode == 0, result.stderr
    assert "variable is not set" not in result.stderr
    gateway = json.loads(result.stdout)["services"]["gateway"]["environment"]
    # `config` re-escapes a literal `$` as `$$` so its output stays re-parseable; the value itself is intact
    assert gateway["DATABASE_URL"] == "postgresql://deerflow:pa$$sword#x@postgres:5432/deerflow"
    assert gateway["DEER_FLOW_STREAM_BRIDGE_REDIS_URL"] == "redis://:ab$${HOME}cd@redis:6379/0"
    # control: the same values bare are rewritten, which is the defect the quoting closes
    env_file.write_text("".join(f"{line.split('=', 1)[0]}={replaced[line.split('=', 1)[0]]}\n" if line.split("=", 1)[0] in replaced else f"{line}\n" for line in lines), encoding="utf-8")
    control = _compose_config(env_file)
    assert control.returncode == 0, control.stderr
    assert "sword" in control.stderr and "variable is not set" in control.stderr
    assert json.loads(control.stdout)["services"]["gateway"]["environment"]["DATABASE_URL"] == "postgresql://deerflow:pa#x@postgres:5432/deerflow"


def test_env_example_lists_exactly_the_fixed_contract_keys() -> None:
    lines = (PROFILE / ".env.example").read_text(encoding="utf-8").splitlines()
    keys = {line.split("=", 1)[0] for line in lines if line and not line.startswith("#")}
    assert keys == CONTRACT_KEYS
    comments = [line for line in lines if line.startswith("#")]
    assert len(comments) == 1 and "verbatim" in comments[0] and "subset" in comments[0]
    values = dict(line.split("=", 1) for line in lines if line and not line.startswith("#"))
    assert values["HARTMESH_TRUSTED_PROXIES"] == "192.0.2.10,192.0.2.11"
    assert values["HARTMESH_PUBLIC_HOST"] == "tenant.example.com"
    assert values["HARTMESH_LISTEN"] == "0.0.0.0:2026"
    assert values["SANDBOX_EGRESS"] in {"allowlist", "open"}
    assert not (PROFILE / ".env").exists()
    assert ".env" in (PROFILE / ".gitignore").read_text(encoding="utf-8").split()


def test_compose_renders_one_subnet_into_both_places_with_and_without_the_override(tmp_path: Path) -> None:
    """The same three properties through the real renderer, on the
    documentation values: an existing contract file that never heard of the
    override still renders and gets the shipped default, an override moves the
    network and the Gateway's trust together, and neither render publishes
    anything but nginx."""
    default = _app_subnet_defaults()[0]
    override = "10.90.7.0/24"
    example = (PROFILE / ".env.example").read_text(encoding="utf-8")
    cases = {default: example, override: f"{example}HARTMESH_APP_SUBNET={override}\n"}
    for expected, body in cases.items():
        env_file = tmp_path / f"{expected.replace('/', '_')}.env"
        env_file.write_text(body, encoding="utf-8")
        result = _compose_config(env_file)
        assert result.returncode == 0, result.stderr
        assert "variable is not set" not in result.stderr
        rendered = json.loads(result.stdout)
        assert rendered["networks"]["app"]["ipam"]["config"][0]["subnet"] == expected
        assert rendered["services"]["gateway"]["environment"]["AUTH_TRUSTED_PROXIES"] == expected
        assert {name for name, service in rendered["services"].items() if service.get("ports")} == {"nginx"}
        assert {name for name, service in rendered["services"].items() if list(service["networks"]) == ["app"]} == SERVICES


def test_profile_consumes_no_key_outside_the_contract() -> None:
    source = COMPOSE.read_text(encoding="utf-8")
    referenced = set(_ENV_REFERENCE.findall(source))
    assert referenced <= CONTRACT_KEYS | OPTIONAL_KEYS, referenced - CONTRACT_KEYS - OPTIONAL_KEYS
    assert referenced >= CONTRACT_KEYS - {"SANDBOX_EGRESS"}, "every fixed key but SANDBOX_EGRESS is interpolated by compose.yaml"
    for key in OPTIONAL_KEYS:
        # An optional key is one an existing tenant .env does not carry, so every
        # reference to it must supply the shipped default itself.
        occurrences = source.count("${" + key)
        assert occurrences and occurrences == len(re.findall(r"\$\{" + key + r":-[^}\s]+\}", source)), f"{key} must be referenced only as ${{{key}:-<default>}} so an existing .env still renders"
    contract_like = re.compile(r"\b(HARTMESH_[A-Z_]+|SANDBOX_[A-Z_]+|POSTGRES_PASSWORD|REDIS_PASSWORD|AUTH_JWT_SECRET)\b")
    seams = {"HARTMESH_RENDER_ONLY", "HARTMESH_NGINX_SOURCE", "HARTMESH_NGINX_TARGET"}
    for path in (PROFILE / "gateway" / "run.sh", PROFILE / "gateway" / "entrypoint.sh", PROFILE / "gateway" / "render_config.py", PROFILE / "nginx" / "render.sh"):
        names = set(contract_like.findall(path.read_text(encoding="utf-8"))) - seams
        assert names <= CONTRACT_KEYS, (path.name, names - CONTRACT_KEYS)


def test_gateway_entrypoint_drops_to_uid_1000_with_the_socket_group_and_runs_one_worker() -> None:
    entrypoint = (PROFILE / "gateway" / "entrypoint.sh").read_text(encoding="utf-8")
    run = (PROFILE / "gateway" / "run.sh").read_text(encoding="utf-8")
    assert 'docker_gid="$(stat -c %g "$SOCKET")"' in entrypoint
    assert 'setpriv --reuid=1000 --regid=1000 --groups="$docker_gid" --inh-caps=-all --no-new-privs sh "$RUN"' in entrypoint
    assert 'if [ "$docker_gid" = "0" ]; then' in entrypoint, "a socket owned by gid 0 must be refused, not granted as a supplementary group"
    assert entrypoint.index('if [ "$docker_gid" = "0" ]') < entrypoint.index("exec setpriv")
    assert "uvicorn app.gateway.app:app --host 0.0.0.0 --port 8001 --workers 1" in run
    assert 'render_config.py" \\' in run and '--output "$DEER_FLOW_CONFIG_PATH"' in run
    assert 'if [ ! -f "$DEER_FLOW_EXTENSIONS_CONFIG_PATH" ]; then' in run
    assert 'cp "$PROFILE/extensions_config.json" "$DEER_FLOW_EXTENSIONS_CONFIG_PATH"' in run


# ── nginx ────────────────────────────────────────────────────────────────────


def test_profile_nginx_conf_is_a_verbatim_copy_of_the_compose_nginx_conf() -> None:
    assert (PROFILE / "nginx" / "nginx.conf").read_bytes() == (REPO_ROOT / "docker" / "nginx" / "nginx.conf").read_bytes()
    source = (PROFILE / "nginx" / "nginx.conf").read_text(encoding="utf-8")
    assert "${" not in source
    assert set(_BARE_VARIABLE.findall(source)) == NGINX_VARIABLES
    assert len(_BARE_VARIABLE.findall(source)) == 99


def _render_nginx(tmp_path: Path, environ: dict[str, str], *, source: Path | None = None) -> subprocess.CompletedProcess[str]:
    target = tmp_path / "nginx.conf"
    env = {"PATH": os.environ["PATH"], "HARTMESH_RENDER_ONLY": "1", "HARTMESH_NGINX_SOURCE": str(source or PROFILE / "nginx" / "nginx.conf"), "HARTMESH_NGINX_TARGET": str(target), **environ}
    return subprocess.run(["sh", str(PROFILE / "nginx" / "render.sh")], env=env, capture_output=True, text=True, timeout=30, check=False)


def test_nginx_render_substitutes_only_the_server_name_and_adds_real_ip_directives(tmp_path: Path) -> None:
    result = _render_nginx(tmp_path, {"HARTMESH_PUBLIC_HOST": "tenant.example.com", "HARTMESH_TRUSTED_PROXIES": "192.0.2.10, 192.0.2.11,2001:db8::/32"})
    assert result.returncode == 0, result.stderr
    rendered = (tmp_path / "nginx.conf").read_text(encoding="utf-8")
    source = (PROFILE / "nginx" / "nginx.conf").read_text(encoding="utf-8")
    assert _BARE_VARIABLE.findall(rendered) == _BARE_VARIABLE.findall(source), "every nginx variable must survive the render"
    assert "server_name _;" not in rendered
    assert "        server_name tenant.example.com;\n" in rendered
    for address in ("192.0.2.10", "192.0.2.11", "2001:db8::/32"):
        assert f"        set_real_ip_from {address};\n" in rendered
    assert "        real_ip_header X-Forwarded-For;\n" in rendered
    assert "        real_ip_recursive on;\n" in rendered
    assert rendered.count("set_real_ip_from ") == 3
    assert rendered.count("real_ip_header ") == 1


def test_nginx_render_refuses_bad_hosts_bad_proxies_and_a_missing_anchor(tmp_path: Path) -> None:
    bad_host = _render_nginx(tmp_path, {"HARTMESH_PUBLIC_HOST": "evil; }", "HARTMESH_TRUSTED_PROXIES": "192.0.2.10"})
    assert bad_host.returncode == 1 and "HARTMESH_PUBLIC_HOST" in bad_host.stderr
    bad_proxy = _render_nginx(tmp_path, {"HARTMESH_PUBLIC_HOST": "tenant.example.com", "HARTMESH_TRUSTED_PROXIES": "192.0.2.10;x"})
    assert bad_proxy.returncode == 1 and "HARTMESH_TRUSTED_PROXIES" in bad_proxy.stderr
    empty = _render_nginx(tmp_path, {"HARTMESH_PUBLIC_HOST": "tenant.example.com", "HARTMESH_TRUSTED_PROXIES": " , "})
    assert empty.returncode == 1
    anchorless = tmp_path / "anchorless.conf"
    anchorless.write_text((PROFILE / "nginx" / "nginx.conf").read_text(encoding="utf-8").replace("server_name _;", "server_name x;"), encoding="utf-8")
    missing = _render_nginx(tmp_path, {"HARTMESH_PUBLIC_HOST": "tenant.example.com", "HARTMESH_TRUSTED_PROXIES": "192.0.2.10"}, source=anchorless)
    assert missing.returncode == 1 and "anchor" in missing.stderr


# ── config.yaml render ───────────────────────────────────────────────────────


def _example_config() -> str:
    return (REPO_ROOT / "config.example.yaml").read_text(encoding="utf-8")


def test_template_matches_the_example_version_provider_and_local_backend(render_config: ModuleType) -> None:
    template = yaml.safe_load(TEMPLATE.read_text(encoding="utf-8"))
    example = yaml.safe_load(_example_config())
    assert template["config_version"] == example["config_version"]
    provider_line = re.search(r"^#\s+use: (deerflow\.community\.aio_sandbox:AioSandboxProvider)$", _example_config(), flags=re.MULTILINE)
    assert provider_line is not None
    assert template["sandbox"]["use"] == provider_line.group(1)
    assert "provisioner_url" not in template["sandbox"]
    assert template["sandbox"]["replicas"] == 2
    assert template["sandbox"]["image"].startswith("ghcr.io/altakleos/hartmesh-sandbox@sha256:"), "the tree carries digest pins between cuts"
    assert template["sandbox"]["network"]["allow_domains"] == ["pypi.org", "files.pythonhosted.org", "registry.npmjs.org", "github.com"]
    assert template["sandbox"]["network"]["approval"] == "prompt"
    assert template["skills"]["path"].startswith("/srv/hartmesh/")
    assert template["auth"]["local"]["lockout_store"] == "redis", "the login lockout must survive a restart so an admin unlock is not an outage (README: Login lockout)"
    assert template["deployment"]["profile"] == "local_development"
    assert template["run_events"]["backend"] == "db"
    assert template["database"]["postgres_url"] == "$DATABASE_URL"


def test_catalog_fragments_are_derived_from_the_example_and_reference_only_their_own_key(render_config: ModuleType) -> None:
    fragments = render_config.load_catalog(CATALOG)
    assert len(fragments) >= 20
    example = _example_config()
    for fragment in fragments:
        assert fragment.env in example, fragment.source
        assert fragment.env.endswith(("_API_KEY", "_APIKEY")), fragment.source
        referenced: set[str] = set()
        render_config._references({"models": list(fragment.models), "tools": list(fragment.tools)}, referenced)
        assert referenced <= {fragment.env}, (fragment.source, referenced)
        for model in fragment.models:
            assert isinstance(model.get("use"), str) and isinstance(model.get("model"), str), fragment.source
        for tool in fragment.tools:
            assert tool["name"] in {"web_search", "web_fetch", "image_search"}, fragment.source
    envs = [fragment.env for fragment in fragments]
    assert len(envs) == len(set(envs))


@pytest.mark.parametrize(
    ("label", "keys", "expected_models", "expected_search"),
    [
        ("none", set(), [], "deerflow.community.ddg_search.tools:web_search_tool"),
        ("one", {"GEMINI_API_KEY"}, ["gemini-2.5-pro"], "deerflow.community.ddg_search.tools:web_search_tool"),
        ("several", {"OPENAI_API_KEY", "ANTHROPIC_API_KEY", "TAVILY_API_KEY", "SERPER_API_KEY"}, ["gpt-4", "gpt-5-responses", "claude-sonnet-4"], "deerflow.community.tavily.tools:web_search_tool"),
    ],
)
def test_render_includes_only_present_providers_and_leaves_no_absent_reference(render_config: ModuleType, label: str, keys: set[str], expected_models: list[str], expected_search: str) -> None:
    environ = {**_base_environ(), **{key: "secret" for key in keys}}
    rendered, included = render_config.render_text(TEMPLATE.read_text(encoding="utf-8"), render_config.load_catalog(CATALOG), environ)
    document = yaml.safe_load(rendered)
    assert [model["name"] for model in document["models"]] == expected_models, label
    tools = {tool["name"]: tool for tool in document["tools"]}
    assert tools["web_search"]["use"] == expected_search
    assert {"web_fetch", "image_search", "ls", "read_file", "glob", "grep", "write_file", "str_replace", "bash"} <= set(tools)
    referenced: set[str] = set()
    render_config._references(document, referenced)
    assert referenced == {"DATABASE_URL", *keys}, label
    assert "secret" not in rendered
    assert {fragment.env for fragment in included} == keys
    assert document["sandbox"]["network"]["mode"] == "allowlist"
    assert "provisioner_url" not in document["sandbox"]


def test_render_selects_the_network_block_from_sandbox_egress(render_config: ModuleType) -> None:
    fragments = render_config.load_catalog(CATALOG)
    template = TEMPLATE.read_text(encoding="utf-8")
    allowlist, _ = render_config.render_text(template, fragments, _base_environ())
    assert yaml.safe_load(allowlist)["sandbox"]["network"]["mode"] == "allowlist"
    absent, _ = render_config.render_text(template, fragments, {**_base_environ(), "SANDBOX_EGRESS": ""})
    assert yaml.safe_load(absent)["sandbox"]["network"]["mode"] == "allowlist"
    opened, _ = render_config.render_text(template, fragments, {**_base_environ(), "SANDBOX_EGRESS": "open"})
    assert yaml.safe_load(opened)["sandbox"]["network"] == {"mode": "open"}
    for bad in ("isolated", "ALLOWLIST", "yes"):
        with pytest.raises(render_config.RenderError, match="SANDBOX_EGRESS"):
            render_config.render_text(template, fragments, {**_base_environ(), "SANDBOX_EGRESS": bad})


def test_render_refuses_a_template_reference_to_an_unset_variable(render_config: ModuleType) -> None:
    with pytest.raises(render_config.RenderError, match="DATABASE_URL"):
        render_config.render_text(TEMPLATE.read_text(encoding="utf-8"), render_config.load_catalog(CATALOG), {})


def test_render_output_is_a_valid_app_config(render_config: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from deerflow.config.app_config import AppConfig

    keys = {"OPENAI_API_KEY", "TAVILY_API_KEY"}
    environ = {**_base_environ(), **{key: "secret" for key in keys}}
    for name, value in environ.items():
        monkeypatch.setenv(name, value)
    rendered, _ = render_config.render_text(TEMPLATE.read_text(encoding="utf-8"), render_config.load_catalog(CATALOG), environ)
    path = tmp_path / "config.yaml"
    path.write_text(rendered, encoding="utf-8")
    config = AppConfig.from_file(str(path))
    assert [model.name for model in config.models] == ["gpt-4", "gpt-5-responses"]
    assert config.sandbox.use == "deerflow.community.aio_sandbox:AioSandboxProvider"
    assert config.sandbox.replicas == 2
    assert config.sandbox.network.mode == "allowlist"
    assert config.run_events.backend == "db"


def test_rendered_profile_carries_the_office_retry_budget(render_config: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The login-throttle policy as the tenant Gateway actually consumes it.

    Asserted on the rendered file rather than on a hand-built LocalAuthConfig,
    because what ships is this template plus the renderer: a value dropped from
    the template, or a renderer that stops copying `auth:` through, would leave
    a policy fixture passing and the tenant on the generic default.

    The profile raises only the total-volume limit. The standalone default of
    300 is exactly the design allowance for this deployment shape -- twenty
    staff, five wrong attempts each, then ten further retries each while their
    account is locked -- so a single bad morning would sit *on* the limit with
    nothing left. 600 is that allowance doubled: 299 further failures before the
    limit trips. Every other control is the standalone default, deliberately.
    """
    from deerflow.config.app_config import AppConfig
    from deerflow.config.auth_config import LocalAuthConfig

    monkeypatch.setenv("DATABASE_URL", _base_environ()["DATABASE_URL"])
    monkeypatch.setenv("DEER_FLOW_STREAM_BRIDGE_REDIS_URL", _base_environ()["DEER_FLOW_STREAM_BRIDGE_REDIS_URL"])
    rendered, _ = render_config.render_text(TEMPLATE.read_text(encoding="utf-8"), render_config.load_catalog(CATALOG), _base_environ())
    path = tmp_path / "config.yaml"
    path.write_text(rendered, encoding="utf-8")
    local = AppConfig.from_file(str(path)).auth.local

    standalone = LocalAuthConfig()
    assert local.source_max_failures == 600
    assert local.source_max_failures == 2 * standalone.source_max_failures, "the profile doubles the generic volume limit and changes nothing else"
    assert local.lockout_store == "redis", "the profile's one other departure (README: 'Login lockout')"
    for field in ("source_max_distinct_accounts", "source_window_seconds", "source_lockout_seconds"):
        assert getattr(local, field) == getattr(standalone, field), field
    assert local.effective_account_max_attempts == standalone.effective_account_max_attempts == 5
    assert local.effective_account_lockout_seconds == standalone.effective_account_lockout_seconds == 300.0


# ── release pinning ──────────────────────────────────────────────────────────


def test_the_tree_carries_digest_pins_between_cuts_and_check_accepts_them(pin_images: ModuleType) -> None:
    references = pin_images.read_references(IMAGES)
    assert all(pin_images.PINNED_REFERENCE.fullmatch(reference) for reference in references), "the last pin commit left digest pins; a candidate build must ignore them"
    assert pin_images.verify(pin_images.ProfileFiles.under(PROFILE)) == references


def test_images_txt_lists_exactly_the_references_the_profile_uses(pin_images: ModuleType) -> None:
    references = pin_images.read_references(IMAGES)
    used = pin_images.yaml_references(COMPOSE) + pin_images.yaml_references(TEMPLATE)
    assert sorted(references) == sorted(set(used))
    assert len(references) == 7
    repositories = {pin_images.repository_of(reference) for reference in references}
    assert repositories == {
        "ghcr.io/altakleos/hartmesh-backend",
        "ghcr.io/altakleos/hartmesh-frontend",
        "ghcr.io/altakleos/hartmesh-sandbox",
        "ghcr.io/altakleos/hartmesh-sandbox-network-proxy",
        "postgres",
        "redis",
        "nginx",
    }
    for repository in repositories:
        assert re.fullmatch(r"[a-z0-9./_-]+", repository), repository


def _fake_resolver(calls: list[str]):
    def resolve(reference: str) -> str:
        calls.append(reference)
        import hashlib

        return "sha256:" + hashlib.sha256(reference.encode()).hexdigest()

    return resolve


PLACEHOLDER_THIRD_PARTY = {"postgres": "postgres:16", "redis": "redis:7-alpine", "nginx": "nginx:alpine"}


def _placeholder_profile(pin_images: ModuleType, tmp_path: Path):
    """A copy of the profile whose references are tag-form placeholders.

    Between cuts the tree carries the previous release's digest pins, so the
    tests build the placeholder state themselves instead of assuming it.
    """

    copy = tmp_path / "compose"
    shutil.copytree(PROFILE, copy)
    files = pin_images.ProfileFiles.under(copy)
    mapping = {}
    for reference in pin_images.read_references(files.images):
        repository = pin_images.repository_of(reference)
        mapping[reference] = f"{repository}:v0.0.0-hartmesh.0" if pin_images.is_fork_image(repository) else PLACEHOLDER_THIRD_PARTY[repository]
    for path in (files.compose, files.config):
        pin_images.rewrite_yaml(path, mapping)
    files.images.write_text("".join(f"{mapping[reference]}\n" for reference in mapping), encoding="utf-8")
    return files


def test_pin_rewrites_every_reference_to_a_digest_and_check_refuses_tag_form(pin_images: ModuleType, tmp_path: Path) -> None:
    files = _placeholder_profile(pin_images, tmp_path)
    copy = files.images.parent
    if any(pin_images.PINNED_REFERENCE.fullmatch(reference) is None for reference in pin_images.read_references(files.images)):
        with pytest.raises(pin_images.PinError, match="tag-form"):
            pin_images.verify(files)
    calls: list[str] = []
    pinned = pin_images.pin(files, _fake_resolver(calls), release=RELEASE).references
    assert len(pinned) == 7
    assert all(pin_images.PINNED_REFERENCE.fullmatch(reference) for reference in pinned)
    assert files.images.read_text(encoding="utf-8") == "".join(f"{reference}\n" for reference in pinned)
    assert set(pin_images.yaml_references(files.compose)) | set(pin_images.yaml_references(files.config)) == set(pinned)
    assert pin_images.verify(files) == pinned
    assert (copy / "compose.yaml").read_text(encoding="utf-8").count("#") == COMPOSE.read_text(encoding="utf-8").count("#"), "comments survive the rewrite"
    again = pin_images.pin(files, _fake_resolver(calls_again := []))
    assert again.references == pinned and again.resolutions == () and calls_again == []
    with pytest.raises(pin_images.PinError, match="not a sha256 digest"):
        garbage = tmp_path / "garbage"
        shutil.copytree(PROFILE, garbage)
        pin_images.pin(pin_images.ProfileFiles.under(garbage), lambda reference: "latest", release=RELEASE)


def test_pin_check_mode_refuses_a_profile_that_disagrees_with_images_txt(pin_images: ModuleType, tmp_path: Path) -> None:
    files = _placeholder_profile(pin_images, tmp_path)
    copy = files.images.parent
    pin_images.pin(files, _fake_resolver([]), release=RELEASE)
    text = files.compose.read_text(encoding="utf-8")
    files.compose.write_text(text.replace("postgres@sha256:", "postgres:16@sha256:", 1), encoding="utf-8")
    with pytest.raises(pin_images.PinError, match="not a line of"):
        pin_images.verify(files)
    result = subprocess.run([sys.executable, str(REPO_ROOT / "scripts" / "pin_compose_images.py"), "--check", "--profile", str(copy)], capture_output=True, text=True, check=False)
    assert result.returncode == 1 and "pin_compose_images:" in result.stderr


def test_release_rewrites_fork_lines_to_the_release_tag_and_leaves_third_party_lines(pin_images: ModuleType, tmp_path: Path) -> None:
    files = _placeholder_profile(pin_images, tmp_path)
    before = pin_images.read_references(files.images)
    fork_before = [reference for reference in before if pin_images.is_fork_image(pin_images.repository_of(reference))]
    third_party_before = [reference for reference in before if reference not in fork_before]
    assert len(fork_before) == 4 and third_party_before == ["postgres:16", "redis:7-alpine", "nginx:alpine"]
    assert all(reference.endswith(f":{RELEASE_IMAGE_TAG}") is False for reference in fork_before), "the tree carries placeholders, not this release"
    calls: list[str] = []
    result = pin_images.pin(files, _fake_resolver(calls), release=RELEASE)
    assert calls == [f"{pin_images.repository_of(reference)}:{RELEASE_IMAGE_TAG}" for reference in fork_before] + third_party_before
    assert [resolution.reference for resolution in result.resolutions] == calls
    for resolution in result.resolutions:
        assert resolution.pinned == f"{pin_images.repository_of(resolution.reference)}@{_fake_resolver([])(resolution.reference)}"
    assert result.references == [resolution.pinned for resolution in result.resolutions]
    assert pin_images.verify(files) == result.references
    # A leading "v" is tolerated and means the same release.
    again = pin_images.pin(files, _fake_resolver(calls_again := []), release=f"v{RELEASE}")
    assert again.references == result.references and calls_again == [f"{pin_images.repository_of(r)}:{RELEASE_IMAGE_TAG}" for r in fork_before]


def test_release_repins_fork_lines_already_pinned_to_an_earlier_release(pin_images: ModuleType, tmp_path: Path) -> None:
    files = _placeholder_profile(pin_images, tmp_path)
    earlier = pin_images.pin(files, _fake_resolver([]), release="2.1.0+hartmesh.4").references
    calls: list[str] = []
    result = pin_images.pin(files, _fake_resolver(calls), release=RELEASE)
    assert calls == [f"{pin_images.repository_of(reference)}:{RELEASE_IMAGE_TAG}" for reference in earlier if pin_images.is_fork_image(pin_images.repository_of(reference))]
    assert result.references != earlier and pin_images.verify(files) == result.references
    assert [reference for reference in result.references if not pin_images.is_fork_image(pin_images.repository_of(reference))] == [reference for reference in earlier if not pin_images.is_fork_image(pin_images.repository_of(reference))]


def test_pin_without_a_release_refuses_tag_form_fork_lines(pin_images: ModuleType, tmp_path: Path) -> None:
    files = _placeholder_profile(pin_images, tmp_path)
    copy = files.images.parent
    original = {path: path.read_text(encoding="utf-8") for path in (files.images, files.compose, files.config)}
    calls: list[str] = []
    with pytest.raises(pin_images.PinError, match="--release"):
        pin_images.pin(files, _fake_resolver(calls))
    assert calls == [], "nothing is resolved before the refusal"
    assert {path: path.read_text(encoding="utf-8") for path in original} == original, "nothing is rewritten before the refusal"
    result = subprocess.run([sys.executable, str(REPO_ROOT / "scripts" / "pin_compose_images.py"), "--profile", str(copy)], capture_output=True, text=True, check=False)
    assert result.returncode == 1 and "--release" in result.stderr
    for bad in ("2.1.0", "hartmesh.5", "2.1.0-hartmesh.5", "v2.1.0+hartmesh.", "2.1.0+hartmesh.5 "):
        with pytest.raises(pin_images.PinError, match="release version"):
            pin_images.pin(files, _fake_resolver([]), release=bad)
    with pytest.raises(pin_images.PinError, match="tag-form"):
        pin_images.verify(files)
    check = subprocess.run([sys.executable, str(REPO_ROOT / "scripts" / "pin_compose_images.py"), "--check", "--release", RELEASE, "--profile", str(copy)], capture_output=True, text=True, check=False)
    assert check.returncode != 0, "--check takes no release: it verifies the tree as it is"


def test_pin_script_is_executable_as_releasing_md_invokes_it() -> None:
    script = REPO_ROOT / "scripts" / "pin_compose_images.py"
    assert script.read_text(encoding="utf-8").startswith("#!/usr/bin/env python3\n")
    assert script.stat().st_mode & 0o111, "RELEASING.md runs scripts/pin_compose_images.py directly"


def test_release_image_tag_spelling_matches_the_shell_helper(pin_images: ModuleType) -> None:
    for version in (RELEASE, "2.1.0+hartmesh.10", "10.0.1+hartmesh.1"):
        helper = subprocess.run(["bash", str(REPO_ROOT / "scripts" / "release_tag_spellings.sh"), version], capture_output=True, text=True, check=True)
        expected = dict(line.split("=", 1) for line in helper.stdout.split())["image_tag"]
        assert pin_images.release_image_tag(version) == expected
        assert pin_images.release_image_tag(f"v{version}") == expected


def test_fork_images_are_the_components_the_container_workflow_builds(pin_images: ModuleType) -> None:
    workflow = yaml.safe_load((REPO_ROOT / ".github" / "workflows" / "container.yaml").read_text(encoding="utf-8"))
    matrix = next(job for job in workflow["jobs"].values() if "strategy" in job)["strategy"]["matrix"]["include"]
    assert {entry["component"] for entry in matrix} == set(pin_images.FORK_COMPONENTS)
    assert pin_images.is_fork_image("ghcr.io/example/fork-backend")
    assert pin_images.is_fork_image("ghcr.io/example/fork-sandbox-network-proxy")
    assert not pin_images.is_fork_image("ghcr.io/example/fork-sandbox-base")
    assert not pin_images.is_fork_image("postgres")
    assert not pin_images.is_fork_image("docker.io/library/nginx")


def _adopt_step() -> tuple[dict, str]:
    workflow = yaml.safe_load((REPO_ROOT / ".github" / "workflows" / "container.yaml").read_text(encoding="utf-8"))
    job = workflow["jobs"]["container"]
    steps = {step.get("name"): step for step in job["steps"]}
    return job, steps["Adopt the digest deploy/compose pins for this component"]["run"]


def test_adopt_step_retags_with_crane_and_asserts_the_digest_before_and_after() -> None:
    job, adopt = _adopt_step()
    assert "imagetools" not in adopt
    assert 'crane manifest "$PIN" >/dev/null' in adopt
    assert 'crane tag "$PIN" "$IMAGE_TAG"' in adopt
    assert 'crane tag "$PIN" "sha-${SHORT_SHA}"' in adopt
    # Before: the candidate build under this version put the pinned digest at the release tag.
    before = 'if [ "$BEFORE" != "$PIN_DIGEST" ]; then'
    assert before in adopt and adopt.index(before) < adopt.index('crane tag "$PIN"')
    assert "resolves to ${BEFORE:-nothing}, not the pinned ${PIN_DIGEST}" in adopt
    # After: both new tags resolve to the pin with the pin's media type.
    after = 'if [ "$AFTER" != "$PIN_DIGEST" ] || [ "$AFTER_MEDIA_TYPE" != "$PIN_MEDIA_TYPE" ]; then'
    assert after in adopt and adopt.index(after) > adopt.index('crane tag "$PIN" "sha-${SHORT_SHA}"')
    assert 'for REF in "$RELEASE_TAG" "$SHA_TAG"; do' in adopt
    assert 'tee -a "$GITHUB_STEP_SUMMARY"' in adopt, "the cut reads the assertion lines back from the log and the summary"
    crane_setup = [step for step in job["steps"] if str(step.get("uses", "")).startswith("imjasonh/setup-crane@feee3b6bb0d4c68370f256a4502498c9227e5c6b")]
    assert len(crane_setup) == 1 and job["steps"].index(crane_setup[0]) < job["steps"].index(next(step for step in job["steps"] if step.get("id") == "adopt"))
    assert 'crane auth login "$REGISTRY" -u "$GITHUB_ACTOR" --password-stdin' in adopt


def test_adopt_step_adopts_only_on_a_tag_push() -> None:
    _, adopt = _adopt_step()
    guard = 'if [ "$GITHUB_EVENT_NAME" != "push" ]; then'
    assert guard in adopt
    assert adopt.index(guard) < adopt.index('PIN="$(grep'), "a candidate build exits before reading the pins"
    guarded = adopt[adopt.index(guard) : adopt.index("fi", adopt.index(guard))]
    assert 'echo "adopted=false" >> "$GITHUB_OUTPUT"' in guarded and "exit 0" in guarded
    assert "run scripts/pin_compose_images.py before tagging the release" in adopt, "a tag-form fork line still fails a tag push"


def test_release_workflows_reference_the_compose_profile() -> None:
    manifest = (REPO_ROOT / ".github" / "workflows" / "release-manifest.yaml").read_text(encoding="utf-8")
    assert "deploy/compose/images.txt" in manifest
    assert "scripts/pin_compose_images.py --check" in manifest
    releasing = (REPO_ROOT / "RELEASING.md").read_text(encoding="utf-8")
    assert "scripts/pin_compose_images.py" in releasing
    assert "deploy/compose/images.txt" in releasing
