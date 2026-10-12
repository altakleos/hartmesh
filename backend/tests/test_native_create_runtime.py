"""Exact managed pool lifetime owns journal qualification through final drainage."""

from types import SimpleNamespace

import pytest

from deerflow.spaces.docker import StorageHostUnqualified
from deerflow.spaces.runtime_owner import PoolRuntimeOwner


@pytest.fixture
def pool(tmp_path):
    root = tmp_path / "pool"
    root.mkdir(mode=0o750)
    (root / ".gateway-owner.lock").touch(mode=0o600)
    (root / ".native-creates").mkdir(mode=0o700)
    return root


def test_pool_retains_one_exact_native_journal_across_qualification_retry(pool):
    owner = PoolRuntimeOwner(pool)
    provider = SimpleNamespace()
    try:
        owner.resolve_provider(lambda: provider)
        first = owner.qualify_native_evidence(provider, daemon_id="daemon-A")
        second = owner.qualify_native_evidence(provider, daemon_id="daemon-A")
        assert first is second and owner.native_create_evidence is first and first.ready
        first.begin(owner_id="alice", thread_id="chat", name="native-chat", slots=("a" * 32,))
        assert owner.qualify_native_evidence(provider, daemon_id="daemon-A") is first
    finally:
        owner.close()
    assert not first.ready


def test_native_qualification_requires_exact_provider_before_journal_lookup(pool):
    owner = PoolRuntimeOwner(pool)
    try:
        with pytest.raises(StorageHostUnqualified):
            owner.qualify_native_evidence(SimpleNamespace(), daemon_id="daemon-A")
        assert owner.native_create_evidence is None
        provider = owner.resolve_provider(SimpleNamespace)
        with pytest.raises(StorageHostUnqualified):
            owner.qualify_native_evidence(SimpleNamespace(), daemon_id="daemon-A")
        assert owner.native_create_evidence is None
        assert owner.qualify_native_evidence(provider, daemon_id="daemon-A").ready
    finally:
        owner.close()


def test_native_folder_unavailable_preserves_owner_provider_for_retry(pool):
    directory = pool / ".native-creates"
    directory.rmdir()
    owner = PoolRuntimeOwner(pool)
    provider = SimpleNamespace()
    try:
        owner.resolve_provider(lambda: provider)
        with pytest.raises(StorageHostUnqualified):
            owner.qualify_native_evidence(provider, daemon_id="daemon-A")
        assert owner.provider is provider and owner.native_create_evidence is None
        directory.mkdir(mode=0o700)
        assert owner.qualify_native_evidence(provider, daemon_id="daemon-A").ready
    finally:
        owner.close()


@pytest.mark.parametrize("changed", ["directory", "daemon"])
def test_requalification_cannot_switch_directory_or_daemon(pool, changed):
    owner = PoolRuntimeOwner(pool)
    provider = SimpleNamespace()
    try:
        owner.resolve_provider(lambda: provider)
        first = owner.qualify_native_evidence(provider, daemon_id="daemon-A")
        attempt = first.begin(owner_id="alice", thread_id="chat", name="native-chat", slots=("a" * 32,))
        if changed == "directory":
            (pool / ".native-creates").rename(pool / "retained-evidence")
            (pool / ".native-creates").mkdir(mode=0o700)
        with pytest.raises(StorageHostUnqualified):
            owner.qualify_native_evidence(provider, daemon_id="daemon-B" if changed == "daemon" else "daemon-A")
        assert owner.native_create_evidence is first and first.ready
        directory = pool / ("retained-evidence" if changed == "directory" else ".native-creates")
        assert (directory / (attempt.id + ".json")).is_file()
    finally:
        owner.close()


def test_closing_blocks_new_native_qualification_but_retains_journal_until_close(pool):
    owner = PoolRuntimeOwner(pool)
    provider = SimpleNamespace()
    try:
        owner.resolve_provider(lambda: provider)
        journal = owner.qualify_native_evidence(provider, daemon_id="daemon-A")
        owner.mark_closing()
        with pytest.raises(StorageHostUnqualified):
            owner.qualify_native_evidence(provider, daemon_id="daemon-A")
        assert journal.ready and owner.owns(provider)
    finally:
        owner.close()
    assert not journal.ready


def test_failed_namespace_load_cannot_switch_to_an_empty_replacement(pool):
    directory = pool / ".native-creates"
    unknown = directory / "unknown"
    unknown.write_bytes(b"retained uncertainty")
    unknown.chmod(0o600)
    owner = PoolRuntimeOwner(pool)
    provider = SimpleNamespace()
    try:
        owner.resolve_provider(lambda: provider)
        with pytest.raises(StorageHostUnqualified):
            owner.qualify_native_evidence(provider, daemon_id="daemon-A")
        assert owner.native_create_evidence is None
        directory.rename(pool / "retained-evidence")
        directory.mkdir(mode=0o700)
        with pytest.raises(StorageHostUnqualified):
            owner.qualify_native_evidence(provider, daemon_id="daemon-A")
        assert owner.native_create_evidence is None
        assert (pool / "retained-evidence/unknown").read_bytes() == b"retained uncertainty"
    finally:
        owner.close()


def test_namespace_replacement_during_initial_constructor_is_refused(pool, monkeypatch):
    from deerflow.spaces import native_create_evidence as module

    owner = PoolRuntimeOwner(pool)
    provider = SimpleNamespace()
    original = module.NativeCreateEvidence

    def changed_before_open(*args, **kwargs):
        (pool / ".native-creates").rename(pool / "retained-evidence")
        (pool / ".native-creates").mkdir(mode=0o700)
        return original(*args, **kwargs)

    monkeypatch.setattr(module, "NativeCreateEvidence", changed_before_open)
    try:
        owner.resolve_provider(lambda: provider)
        with pytest.raises(StorageHostUnqualified):
            owner.qualify_native_evidence(provider, daemon_id="daemon-A")
        assert owner.native_create_evidence is None
        assert (pool / "retained-evidence").is_dir()
    finally:
        owner.close()
