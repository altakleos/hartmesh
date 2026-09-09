"""Offline contracts for the development stack's pinned network.

``docker/docker-compose-dev.yaml`` pins ``deer-flow-dev`` so the five dev
services get a stable range instead of whatever Docker's default pool happens
to hand out. It pinned ``192.168.200.0/24`` until 2026-09-09, which is one of
the operator's own external ranges: a developer running ``make docker-start``
on a host inside that network installed a connected route over it and lost the
real one, the same defect the tenant VM profile carried on ``172.30.10.0/24``.

These tests pin the replacement without a Docker daemon: that the network is
still pinned at all, that the range clears every recorded external prefix and
Docker's own pools, that it stays disjoint from the tenant profile's default so
one machine can run both, and that the override an operator needs when even
that collides cannot become a required value.
"""

from __future__ import annotations

import re
from ipaddress import ip_network
from pathlib import Path

import yaml
from _compose_network_ranges import DOCKER_DEFAULT_POOLS, EXTERNAL_RANGES

REPO_ROOT = Path(__file__).resolve().parents[2]
DEV_COMPOSE = REPO_ROOT / "docker" / "docker-compose-dev.yaml"
TENANT_COMPOSE = REPO_ROOT / "deploy" / "compose" / "compose.yaml"
NETWORK = "deer-flow-dev"
OVERRIDE = "DEER_FLOW_DEV_SUBNET"
_DEV_SUBNET = re.compile(r"\$\{" + OVERRIDE + r":-([^}]+)\}")
_TENANT_SUBNET = re.compile(r"\$\{HARTMESH_APP_SUBNET:-([^}]+)\}")


def _dev_compose() -> dict:
    return yaml.safe_load(DEV_COMPOSE.read_text(encoding="utf-8"))


def _dev_subnet_defaults() -> list[str]:
    return _DEV_SUBNET.findall(DEV_COMPOSE.read_text(encoding="utf-8"))


def test_the_dev_network_is_still_pinned_and_carries_every_service() -> None:
    """Unpinning is not the fix. Docker's default pools march through
    172.30.0.0/16 and 172.31.0.0/16, so letting the daemon choose would make the
    collision nondeterministic rather than remove it."""
    compose = _dev_compose()
    network = compose["networks"][NETWORK]
    assert network["driver"] == "bridge"
    assert len(network["ipam"]["config"]) == 1
    for name, service in compose["services"].items():
        assert service.get("networks") == [NETWORK], name


def test_the_dev_subnet_default_clears_every_recorded_external_range() -> None:
    defaults = set(_dev_subnet_defaults())
    assert len(defaults) == 1, defaults
    subnet = ip_network(defaults.pop())
    assert subnet.version == 4 and subnet.is_private
    assert subnet.num_addresses >= 16, "five services plus the bridge address must fit"
    for entry in EXTERNAL_RANGES:
        assert not subnet.overlaps(ip_network(entry)), f"the dev bridge would swallow the route to {entry}"
    for entry in DOCKER_DEFAULT_POOLS:
        assert not subnet.overlaps(ip_network(entry)), f"pinning inside {entry} collides with, or spends, the pool unpinned networks draw from"


def test_the_dev_subnet_is_disjoint_from_the_tenant_profile_default() -> None:
    """One machine may run the development stack and a tenant VM profile side
    by side; two pinned bridges that overlap would fight over the same route."""
    dev = ip_network(_dev_subnet_defaults()[0])
    tenant = ip_network(_TENANT_SUBNET.findall(TENANT_COMPOSE.read_text(encoding="utf-8"))[0])
    assert not dev.overlaps(tenant), f"{dev} overlaps the tenant profile's {tenant}"


def test_the_dev_subnet_override_is_optional_and_always_defaulted() -> None:
    """Existing developer configuration must keep rendering. The override is
    read from the shell or docker/.env, neither of which anyone has today, so
    every reference has to supply the shipped default itself."""
    source = DEV_COMPOSE.read_text(encoding="utf-8")
    occurrences = source.count("${" + OVERRIDE)
    assert occurrences == 1
    assert occurrences == len(re.findall(r"\$\{" + OVERRIDE + r":-[^}\s]+\}", source))
    assert "${" + OVERRIDE + ":?" not in source


def test_moving_the_network_left_the_published_surface_alone() -> None:
    """The bind-address default is a separate contract (see
    test_compose_default_bind_host.py); this only pins that the network change
    did not disturb it, and that no service gained a published port."""
    services = _dev_compose()["services"]
    published = {name: service.get("ports") for name, service in services.items() if service.get("ports")}
    assert published == {"nginx": ["${BIND_HOST:-127.0.0.1}:${PORT:-2026}:2026"]}
