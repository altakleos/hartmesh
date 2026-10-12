"""Managed pool ownership is a retained filesystem capability, not a flag."""

import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from types import SimpleNamespace

import pytest

from deerflow.spaces.docker import StorageHostUnqualified


@pytest.fixture
def pool(tmp_path):
    root = tmp_path / "spaces"
    root.mkdir(mode=0o750)
    path = root / ".gateway-owner.lock"
    path.touch(mode=0o600)
    return root


def owner(pool):
    from deerflow.spaces.runtime_owner import PoolRuntimeOwner

    return PoolRuntimeOwner(pool)


def test_independent_runtime_cannot_acquire_the_same_pool(pool):
    first, second = owner(pool), owner(pool)
    try:
        first.acquire()
        with pytest.raises(StorageHostUnqualified):
            second.acquire()
        first.close()
        second.acquire()
    finally:
        first.close()
        second.close()


@pytest.mark.parametrize("kind", ["symlink", "hardlink", "fifo", "directory", "mode", "nonempty", "missing"])
def test_malformed_runtime_lock_is_never_repaired_by_gateway(pool, kind):
    path = pool / ".gateway-owner.lock"
    if kind in ("symlink", "fifo", "directory", "missing"):
        path.unlink()
    if kind == "symlink":
        target = pool / "target"
        target.write_bytes(b"retain")
        path.symlink_to(target)
    elif kind == "hardlink":
        os.link(path, pool / "another-name")
    elif kind == "fifo":
        os.mkfifo(path)
    elif kind == "directory":
        path.mkdir()
    elif kind == "mode":
        path.chmod(0o660)
    elif kind == "nonempty":
        path.write_bytes(b"unrecognized evidence")
    runtime = owner(pool)
    try:
        with pytest.raises(StorageHostUnqualified):
            runtime.acquire()
        if kind in ("symlink", "nonempty"):
            assert path.read_bytes() == (b"retain" if kind == "symlink" else b"unrecognized evidence")
        if kind == "missing":
            assert not path.exists()
    finally:
        runtime.close()


def test_missing_ownership_can_retry_without_constructing_provider(pool):
    path = pool / ".gateway-owner.lock"
    path.unlink()
    runtime = owner(pool)
    calls = []
    try:
        with pytest.raises(StorageHostUnqualified):
            runtime.resolve_provider(lambda: calls.append(True))
        assert calls == []
        path.touch(mode=0o600)
        provider = SimpleNamespace()
        assert runtime.resolve_provider(lambda: provider) is provider
        runtime.assert_provider(provider)
        with pytest.raises(StorageHostUnqualified):
            runtime.assert_provider(SimpleNamespace())
    finally:
        runtime.close()


def test_requalification_reuses_exact_provider_and_retained_owner(pool):
    runtime = owner(pool)
    calls = []
    provider = SimpleNamespace()
    try:
        assert runtime.resolve_provider(lambda: calls.append(True) or provider) is provider
        assert runtime.resolve_provider(lambda: pytest.fail("A retry constructed another provider")) is provider
        assert calls == [True]
        second = owner(pool)
        try:
            with pytest.raises(StorageHostUnqualified):
                second.acquire()
        finally:
            second.close()
    finally:
        runtime.close()


def test_renamed_lock_never_switches_a_live_runtime_to_another_inode(pool):
    runtime = owner(pool)
    provider = SimpleNamespace()
    try:
        runtime.resolve_provider(lambda: provider)
        path = pool / ".gateway-owner.lock"
        path.rename(pool / "retained-owner")
        path.touch(mode=0o600)
        with pytest.raises(StorageHostUnqualified):
            runtime.resolve_provider(lambda: pytest.fail("Runtime switched to replacement ownership"))
        with pytest.raises(StorageHostUnqualified):
            runtime.assert_provider(provider)
    finally:
        runtime.close()


def test_shutdown_refuses_new_qualification_without_releasing_exclusion(pool):
    runtime, second = owner(pool), owner(pool)
    try:
        runtime.resolve_provider(lambda: SimpleNamespace())
        runtime.mark_closing()
        with pytest.raises(StorageHostUnqualified):
            runtime.resolve_provider(lambda: pytest.fail("Closing runtime created a provider"))
        with pytest.raises(StorageHostUnqualified):
            second.acquire()
        runtime.close()
        second.acquire()
    finally:
        runtime.close()
        second.close()


def test_concurrent_qualification_constructs_only_one_provider(pool):
    runtime = owner(pool)
    entered, resume = Event(), Event()
    provider = SimpleNamespace()
    calls = []

    def construct():
        calls.append(True)
        entered.set()
        assert resume.wait(5)
        return provider

    try:
        with ThreadPoolExecutor(max_workers=2) as workers:
            first = workers.submit(runtime.resolve_provider, construct)
            assert entered.wait(5)
            second = workers.submit(runtime.resolve_provider, construct)
            resume.set()
            assert first.result(5) is second.result(5) is provider
        assert calls == [True]
    finally:
        resume.set()
        runtime.close()


@pytest.mark.parametrize("action", ["qualify", "run"])
def test_changed_pool_permissions_refuse_work_without_releasing_original_owner(pool, action):
    runtime, second = owner(pool), owner(pool)
    try:
        runtime.resolve_provider(lambda: SimpleNamespace())
        pool.chmod(0o770)
        with pytest.raises(StorageHostUnqualified):
            if action == "qualify":
                runtime.resolve_provider(lambda: pytest.fail("Unsafe pool constructed a provider"))
            else:
                runtime.run([sys.executable, "-c", "raise AssertionError('Unsafe pool ran a child')"])
        pool.chmod(0o750)
        with pytest.raises(StorageHostUnqualified):
            second.acquire()
    finally:
        runtime.close()
        second.close()


def test_shutdown_during_constructor_retains_provider_for_draining(pool):
    runtime, second = owner(pool), owner(pool)
    entered, resume = Event(), Event()
    provider = SimpleNamespace()

    def construct():
        entered.set()
        assert resume.wait(5)
        return provider

    try:
        with ThreadPoolExecutor(max_workers=1) as workers:
            future = workers.submit(runtime.resolve_provider, construct)
            assert entered.wait(5)
            runtime.mark_closing()
            with pytest.raises(StorageHostUnqualified):
                second.acquire()
            resume.set()
            with pytest.raises(StorageHostUnqualified):
                future.result(5)
        assert runtime.provider is provider
        with pytest.raises(StorageHostUnqualified):
            second.acquire()
    finally:
        resume.set()
        runtime.close()
        second.close()


def test_lifecycle_runner_passes_owned_lock_to_its_child(pool):
    runtime = owner(pool)
    try:
        runtime.acquire()
        script = (
            "import os,sys; target=os.stat(sys.argv[1]); found=[]\n"
            "for name in os.listdir('/proc/self/fd'):\n"
            " try: info=os.fstat(int(name))\n except OSError: continue\n"
            " if (info.st_dev,info.st_ino)==(target.st_dev,target.st_ino): found.append(int(name))\n"
            "assert found, 'No retained ownership descriptor reached child'\n"
        )
        runtime.run([sys.executable, "-c", script, str(pool / ".gateway-owner.lock")], check=True, timeout=5)
    finally:
        runtime.close()


def test_surviving_child_retains_exclusion_after_its_launcher_is_killed(pool, tmp_path):
    import time

    ready, resume, done = (tmp_path / name for name in ("ready", "resume", "done"))
    child_script = (
        "import os,sys,time; from pathlib import Path; ready,resume,done=map(Path,sys.argv[1:]); ready.write_text(str(os.getpid())); deadline=time.monotonic()+10\n"
        "while not resume.exists():\n assert time.monotonic()<deadline\n time.sleep(.01)\ndone.touch()\n"
    )
    launcher_script = (
        "import sys; from deerflow.spaces.runtime_owner import PoolRuntimeOwner; from pathlib import Path; owner=PoolRuntimeOwner(Path(sys.argv[1])); owner.acquire(); "
        "owner.run([sys.executable,'-c',sys.argv[2],*sys.argv[3:]],check=True,timeout=15)"
    )
    second = owner(pool)
    launcher = subprocess.Popen([sys.executable, "-c", launcher_script, str(pool), child_script, str(ready), str(resume), str(done)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        deadline = time.monotonic() + 5
        while not ready.exists():
            assert launcher.poll() is None, launcher.communicate()
            assert time.monotonic() < deadline
            time.sleep(0.01)
        launcher.terminate()
        launcher.wait(timeout=5)
        with pytest.raises(StorageHostUnqualified):
            second.acquire()
        resume.touch()
        deadline = time.monotonic() + 5
        while True:
            try:
                second.acquire()
            except StorageHostUnqualified:
                assert time.monotonic() < deadline
                time.sleep(0.01)
            else:
                break
        assert done.exists()
    finally:
        resume.touch()
        if launcher.poll() is None:
            launcher.terminate()
            launcher.wait(timeout=5)
        launcher.communicate(timeout=5)
        second.close()


@pytest.mark.parametrize("foreign_host", [False, True])
def test_existing_provider_is_never_recorded_or_rebound_by_another_owner(pool, tmp_path, foreign_host):
    first = owner(pool)
    other_pool = tmp_path / "other-pool"
    other_pool.mkdir(mode=0o750)
    (other_pool / ".gateway-owner.lock").touch(mode=0o600)
    from deerflow.spaces.runtime_owner import PoolRuntimeOwner

    second = PoolRuntimeOwner(other_pool)
    provider = SimpleNamespace()
    if foreign_host:
        provider._chat_storage_host = SimpleNamespace(pool_runtime_owner=first)
    else:
        first.resolve_provider(lambda: provider)
        provider._pool_runtime_owner = first
    try:
        with pytest.raises(StorageHostUnqualified):
            second.resolve_provider(lambda: provider)
        assert second.provider is None
        if not foreign_host:
            assert provider._pool_runtime_owner is first
            first.assert_provider(provider)
    finally:
        first.close()
        second.close()
