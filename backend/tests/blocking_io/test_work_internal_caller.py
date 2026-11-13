"""Shared owner activation retains asynchronous SQL and launch ownership."""

import pytest
from _storage_spaces_test_support import FixtureVolume
from test_agent_instances import instances as instances
from test_agent_work_records import work as work
from test_resident_registry import registry as registry
from test_work_execution import execution as execution
from test_work_internal_caller import test_inprocess_actor_same_results as parity
from test_work_internal_caller import test_owner_activation_preserves_retained_launch_ownership as retained


@pytest.fixture(autouse=True)
def prepared_volume_facts(monkeypatch):
    # Production volumes already carry prepared inode facts. This older SQL
    # fixture computes properties lazily, so capture its real facts at fixture
    # construction rather than adding filesystem I/O to a production caller.
    initialize = FixtureVolume.__post_init__

    def prepare(volume):
        initialize(volume)
        volume.prepared_root_inode = volume.data_path.stat().st_ino
        volume.prepared_control_inode = volume.control_path.stat().st_ino

    monkeypatch.setattr(FixtureVolume, "__post_init__", prepare)
    monkeypatch.setattr(FixtureVolume, "root_inode", property(lambda volume: volume.prepared_root_inode))
    monkeypatch.setattr(FixtureVolume, "control_inode", property(lambda volume: volume.prepared_control_inode))


@pytest.mark.asyncio
async def test_owner_activation_matches_http_without_blocking(execution, monkeypatch):
    await parity(execution, monkeypatch, "activate", "http")


@pytest.mark.asyncio
async def test_cancelled_owner_activation_drains_without_blocking(execution, registry, monkeypatch):
    await retained(execution, registry, monkeypatch, "cancellation")
