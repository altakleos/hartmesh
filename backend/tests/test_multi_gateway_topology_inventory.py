from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.gateway.deps import build_multi_gateway_topology_service_registry
from deerflow.config.reload_boundary import STARTUP_ONLY_FIELDS
from deerflow.deployment.topology import (
    MULTI_GATEWAY_ADDITIONAL_CONFIG_FIELDS,
    TOPOLOGY_INVENTORY_CLASSIFICATIONS,
    TopologyError,
    load_topology_inventory,
    validate_topology_inventory_runtime_state,
)
from deerflow.runtime.runs.store.base import LeaseClockAuthority

_INVENTORY = Path(__file__).resolve().parents[2] / "contracts" / "deployment" / "durable_two_gateway_v1.topology.json"
_BACKEND = Path(__file__).resolve().parents[1]


def test_exact_two_gateway_inventory_is_complete_and_classified() -> None:
    inventory = load_topology_inventory(_INVENTORY)

    assert inventory.profile == "durable_two_gateway_v1"
    assert inventory.scope == "durable_two_gateway_v1_postgres_redis_aio_rwx"
    assert inventory.replica_count == 2
    assert len(inventory.dependencies) >= 20
    assert {item.classification for item in inventory.dependencies} <= TOPOLOGY_INVENTORY_CLASSIFICATIONS

    covered_config = {field for dependency in inventory.dependencies for field in dependency.config_fields}
    assert covered_config == set(STARTUP_ONLY_FIELDS) | set(MULTI_GATEWAY_ADDITIONAL_CONFIG_FIELDS)

    covered_services = {service for dependency in inventory.dependencies for service in dependency.service_registrations}
    assert covered_services == set(
        build_multi_gateway_topology_service_registry().names,
    )
    assert all(dependency.service_registrations for dependency in inventory.dependencies)

    registered_runtime_state = {
        match
        for relative in ("app/gateway/app.py", "app/gateway/deps.py")
        for match in re.findall(
            r"\bapp\.state\.([a-z][a-z0-9_]*)",
            (_BACKEND / relative).read_text(encoding="utf-8"),
        )
    }
    inventoried_runtime_state = {field for dependency in inventory.dependencies for field in dependency.runtime_state_fields}
    assert inventoried_runtime_state == registered_runtime_state


def test_inventory_has_no_duplicate_authorities_or_unbounded_text() -> None:
    inventory = load_topology_inventory(_INVENTORY)

    identifiers = [item.id for item in inventory.dependencies]
    assert len(identifiers) == len(set(identifiers))
    for dependency in inventory.dependencies:
        assert dependency.authority
        assert dependency.required_adapter
        assert dependency.requirement
        assert len(dependency.requirement.encode("utf-8")) <= 512


def test_run_store_inventory_pins_database_lease_clock_authority() -> None:
    inventory = load_topology_inventory(_INVENTORY)
    dependency = next(item for item in inventory.dependencies if item.id == "run_manager_store_and_worker_heartbeat")

    assert "lease_clock=database_v1" in dependency.required_adapter
    assert "PostgreSQL database time" in dependency.requirement


def test_inventory_is_a_startup_guard_for_built_runtime_state() -> None:
    inventory = load_topology_inventory(_INVENTORY)
    required = {field for dependency in inventory.dependencies for field in dependency.runtime_state_fields}
    state = type("RuntimeState", (), {})()
    for field in required:
        setattr(state, field, object())
    state.run_store = SimpleNamespace(
        lease_clock_authority=LeaseClockAuthority.database_v1,
    )
    state.topology_service_registry = build_multi_gateway_topology_service_registry()

    assert validate_topology_inventory_runtime_state(state, path=_INVENTORY) == inventory
    delattr(state, sorted(required)[0])
    with pytest.raises(TopologyError, match="topology_dependency_not_shared"):
        validate_topology_inventory_runtime_state(state, path=_INVENTORY)


@pytest.mark.parametrize(
    "run_store",
    [
        SimpleNamespace(
            lease_clock_authority=LeaseClockAuthority.process_v1,
        ),
        SimpleNamespace(),
    ],
)
def test_inventory_rejects_run_store_without_database_lease_clock(
    run_store: object,
) -> None:
    inventory = load_topology_inventory(_INVENTORY)
    required = {field for dependency in inventory.dependencies for field in dependency.runtime_state_fields}
    state = type("RuntimeState", (), {})()
    for field in required:
        setattr(state, field, object())
    state.run_store = run_store
    state.topology_service_registry = build_multi_gateway_topology_service_registry()

    with pytest.raises(TopologyError, match="topology_dependency_not_shared"):
        validate_topology_inventory_runtime_state(state, path=_INVENTORY)
