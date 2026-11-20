"""A fixed-budget child owns each installed projection from start to reap."""

import asyncio
import hashlib
import importlib
import os
import signal
import sys
import threading

import pytest
from _projection_support import installed_or_checkout_package

from deerflow.extensions import projection_host, projection_worker
from deerflow.extensions.projection_host import ProjectionError, ProjectionHost, projector_reference


@pytest.fixture
def projectors(tmp_path, monkeypatch):
    source = tmp_path / "installed_preview_fixture.py"
    source.write_text(
        "import os, time\n"
        "def echo(raw): return raw\n"
        "def child(raw): return str(os.getpid()).encode()\n"
        "def refuse(raw): raise ValueError('private diagnostic')\n"
        "def bad_type(raw): return 3\n"
        "def noisy(raw): print('plugin diagnostic', flush=True); return raw\n"
        "def limits(raw):\n"
        " import json, resource\n"
        " names = ('AS', 'CPU', 'FSIZE', 'NPROC', 'NOFILE', 'CORE')\n"
        " return json.dumps([resource.getrlimit(getattr(resource, 'RLIMIT_' + n))[0] for n in names] + [os.environ.get('PREVIEW_TEST_KEY')]).encode()\n"
        "def delayed(raw): time.sleep(0.6); return raw\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delitem(sys.modules, "installed_preview_fixture", raising=False)
    try:
        yield importlib.import_module("installed_preview_fixture")
    finally:
        sys.modules.pop("installed_preview_fixture", None)


def request(host, function, raw=b"example", *, fingerprint_raw=None, preview_limit=64, requester_id=None):
    async def fingerprint():
        value = raw if fingerprint_raw is None else fingerprint_raw
        return hashlib.sha256(value).hexdigest(), len(value)

    async def read():
        return raw

    return host.project(key=("example", function.__name__, "v1"), projector=function, source_max_bytes=64, preview_max_bytes=preview_limit, fingerprint=fingerprint, read_source=read, requester_id=requester_id)


def test_requires_importable_module_function(projectors):
    assert projector_reference(projectors.echo) == (projectors.echo.__module__, "echo")
    for invalid in (lambda raw: raw, str.encode, os.getpid):
        with pytest.raises(ValueError):
            projector_reference(invalid)


@pytest.mark.parametrize("preload_installed", [True, False], ids=["installed-before-fixture", "checkout-only"])
@pytest.mark.asyncio
async def test_extension_fixture_uses_one_package_for_gateway_and_child(tmp_path, monkeypatch, preload_installed):
    name = "projection_test_package"
    installed = tmp_path / "installed"
    checkout = tmp_path / "checkout"
    for root in (installed, checkout):
        package = root / name
        package.mkdir(parents=True)
        (package / "__init__.py").write_text("", encoding="utf-8")
        (package / "projector.py").write_text(f"def echo(raw): return {root.name.encode()!r} + raw", encoding="utf-8")
    if preload_installed:
        monkeypatch.syspath_prepend(str(installed))
        importlib.import_module(name + ".projector")
    try:
        with installed_or_checkout_package(name, checkout):
            function = importlib.import_module(name + ".projector").echo
            host = ProjectionHost()
            host.start([function])
            try:
                await host.wait_ready()
                content, _, _ = await request(host, function, b"-example")
                assert content == (b"installed-example" if preload_installed else b"checkout-example")
            finally:
                await host.close()
        assert str(checkout) not in sys.path
        assert (name in sys.modules) == preload_installed
        assert (name + ".projector" in sys.modules) == preload_installed
    finally:
        sys.modules.pop(name + ".projector", None)
        sys.modules.pop(name, None)


@pytest.mark.skipif(os.name != "posix", reason="POSIX process credentials")
def test_root_child_drops_all_groups_and_uid_before_loading_code(monkeypatch):
    import pwd

    nobody = pwd.getpwnam("nobody")
    calls = []
    monkeypatch.setattr(os, "getuid", lambda: nobody.pw_uid if ("uid", nobody.pw_uid) in calls else 0)
    monkeypatch.setattr(os, "geteuid", os.getuid)
    monkeypatch.setattr(os, "getgid", lambda: nobody.pw_gid)
    monkeypatch.setattr(os, "getegid", os.getgid)
    monkeypatch.setattr(os, "getgroups", lambda: [])
    monkeypatch.setattr(os, "setgroups", lambda groups: calls.append(("groups", groups)))
    monkeypatch.setattr(os, "setgid", lambda gid: calls.append(("gid", gid)))
    monkeypatch.setattr(os, "setuid", lambda uid: calls.append(("uid", uid)))
    projection_worker._drop_root_identity()
    assert calls == [("groups", []), ("gid", nobody.pw_gid), ("uid", nobody.pw_uid)]
    calls.clear()
    monkeypatch.setattr(os, "getgroups", lambda: [0])
    with pytest.raises(ValueError):
        projection_worker._drop_root_identity()


@pytest.mark.asyncio
async def test_startup_snapshot_identity_and_memo(projectors):
    host = ProjectionHost()
    host.start([projectors.echo, projectors.child])
    try:
        with pytest.raises(ProjectionError) as pending:
            await request(host, projectors.echo)
        assert pending.value.status == 503 and pending.value.retry_after
        await host.wait_ready()
        content, digest, length = await request(host, projectors.echo, b"changed", fingerprint_raw=b"old")
        assert (content, digest, length) == (b"changed", hashlib.sha256(b"changed").hexdigest(), 7)
        first, second = await asyncio.gather(request(host, projectors.child), request(host, projectors.child))
        assert first == second
        assert int(first[0]) != os.getpid()
        with pytest.raises(ProjectionError) as absent:
            await request(host, projectors.refuse)
        assert absent.value.status == 501
    finally:
        await host.close()


@pytest.mark.asyncio
async def test_worker_refusals_and_bounded_output(projectors):
    host = ProjectionHost()
    host.start([projectors.echo, projectors.refuse, projectors.bad_type, projectors.noisy])
    try:
        await host.wait_ready()
        for function, limit, status in ((projectors.echo, 4, 413), (projectors.refuse, 64, 422), (projectors.bad_type, 64, 422)):
            with pytest.raises(ProjectionError) as failure:
                await request(host, function, preview_limit=limit)
            assert failure.value.status == status
        assert (await request(host, projectors.echo, b"abcd", preview_limit=4))[0] == b"abcd"
        with pytest.raises(ProjectionError):
            await request(host, projectors.noisy)
    finally:
        await host.close()


@pytest.mark.skipif(os.name != "posix", reason="POSIX process limits")
@pytest.mark.asyncio
async def test_child_observes_live_kernel_limits_and_empty_environment(projectors, monkeypatch):
    import json

    host = ProjectionHost()
    monkeypatch.setenv("PREVIEW_TEST_KEY", "parent-only")
    host.start([projectors.limits])
    try:
        await host.wait_ready()
        content, _, _ = await request(host, projectors.limits, preview_limit=256)
        assert json.loads(content) == [256 * 1024 * 1024, 8, 0, 0, 16, 0, None]
    finally:
        await host.close()


@pytest.mark.skipif(os.name != "posix", reason="POSIX CPU accounting")
def test_cpu_self_test_finishes_with_kernel_cpu_signal(monkeypatch):
    processes = []
    popen = projection_host.subprocess.Popen

    def observe(*args, **kwargs):
        process = popen(*args, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(projection_host.subprocess, "Popen", observe)
    with pytest.raises(ProjectionError) as refused:
        projection_host._invoke("self-test-cpu", None, b"", 0, 128, 6, threading.Event())
    assert refused.value.status == 422
    assert processes[0].returncode == -signal.SIGXCPU


@pytest.mark.asyncio
async def test_quota_preserves_warm_results_and_refusals_expire(projectors, monkeypatch):
    host = ProjectionHost()
    host.start([projectors.echo, projectors.bad_type])
    try:
        await host.wait_ready()
        first = await request(host, projectors.echo)
        calls = []
        run = host._run

        async def observed(mode, *args):
            calls.append(mode)
            return await run(mode, *args)

        monkeypatch.setattr(host, "_run", observed)
        for _ in range(2):
            with pytest.raises(ProjectionError) as refused:
                await request(host, projectors.bad_type)
            assert refused.value.status == 422
        assert calls == ["project"]
        monkeypatch.setattr(projection_host, "REFUSAL_SECONDS", 0)
        for _ in range(2):
            with pytest.raises(ProjectionError) as refused:
                await request(host, projectors.bad_type, b"another")
            assert refused.value.status == 422
        assert calls == ["project"] * 3

        now = projection_host.time.monotonic()
        host._usage.clear()
        host._usage.append((now - 21, now))
        assert await request(host, projectors.echo) == first
        with pytest.raises(ProjectionError) as busy:
            await request(host, projectors.echo, b"new")
        assert busy.value.status == 503 and busy.value.retry_after > 0
        assert calls == ["project"] * 3
        host._usage.clear()
        host._usage.append((now - 100, now - 65))
        assert (await request(host, projectors.echo, b"new"))[0] == b"new"
    finally:
        await host.close()


@pytest.mark.asyncio
async def test_both_memo_limits_evict_oldest_result(projectors, monkeypatch):
    import itertools

    host = ProjectionHost()
    host.start([projectors.echo])
    try:
        await host.wait_ready()
        calls = itertools.count()
        run = host._run

        async def observed(mode, *args):
            next(calls)
            return await run(mode, *args)

        monkeypatch.setattr(host, "_run", observed)
        monkeypatch.setattr(projection_host, "MEMO_COUNT", 2)
        monkeypatch.setattr(projection_host, "MEMO_BYTES", 6)
        for raw in (b"a", b"b", b"c", b"b", b"a", b"large", b"hi", b"large"):
            assert (await request(host, projectors.echo, raw))[0] == raw
        assert next(calls) == 7
    finally:
        await host.close()


@pytest.mark.asyncio
async def test_admission_is_bounded_before_source_hashing_and_before_read(projectors, monkeypatch):
    host = ProjectionHost()
    host.start([projectors.echo])
    entered = asyncio.Event()
    release = asyncio.Event()
    counters = {"hash": 0, "read": 0}

    async def fingerprint():
        counters["hash"] += 1
        if counters["hash"] == 3:
            entered.set()
        await release.wait()
        return hashlib.sha256(b"demo").hexdigest(), 4

    async def read():
        counters["read"] += 1
        return b"demo"

    async def ask():
        return await host.project(key=("package", "example", "v1"), projector=projectors.echo, source_max_bytes=4, preview_max_bytes=4, fingerprint=fingerprint, read_source=read)

    monkeypatch.setattr(projection_host, "MAX_WAITERS", 2)
    try:
        await host.wait_ready()
        admitted = [asyncio.create_task(ask()) for _ in range(3)]
        await asyncio.wait_for(entered.wait(), 2)
        with pytest.raises(ProjectionError) as busy:
            await ask()
        assert busy.value.status == 503 and busy.value.retry_after
        assert counters == {"hash": 3, "read": 0}
        release.set()
        assert all(answer[0] == b"demo" for answer in await asyncio.gather(*admitted))
        assert counters == {"hash": 3, "read": 1}
    finally:
        release.set()
        await host.close()


@pytest.mark.asyncio
async def test_deadline_and_cancellation_reap_before_slot_reuse(projectors, monkeypatch, caplog):
    host = ProjectionHost()
    host.start([projectors.delayed, projectors.echo])
    try:
        await host.wait_ready()
        started = asyncio.Event()
        procs = []
        loop = asyncio.get_running_loop()
        popen = projection_host.subprocess.Popen

        def observe(*args, **kwargs):
            owned = popen(*args, **kwargs)
            procs.append(owned)
            loop.call_soon_threadsafe(started.set)
            return owned

        monkeypatch.setattr(projection_host.subprocess, "Popen", observe)
        caplog.clear()
        task = asyncio.create_task(request(host, projectors.delayed, requester_id="reader-1"))
        await asyncio.wait_for(started.wait(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert procs[-1].returncode is not None
        monkeypatch.setattr(projection_host, "WALL_SECONDS", 0.16)
        with pytest.raises(ProjectionError) as busy:
            await request(host, projectors.delayed, requester_id="reader-1")
        assert busy.value.status == 503 and procs[-1].returncode is not None
        digest = hashlib.sha256(b"example").hexdigest()
        assert caplog.messages == [f"Preview worker stopped: account=reader-1 sha256={digest} reason={reason}" for reason in ("cancelled", "wall-deadline")]
        monkeypatch.setattr(projection_host, "WALL_SECONDS", 10)
        if os.name == "posix":
            started.clear()
            task = asyncio.create_task(request(host, projectors.delayed, requester_id="reader-1"))
            await asyncio.wait_for(started.wait(), 2)
            procs[-1].kill()
            with pytest.raises(ProjectionError) as interrupted:
                await task
            assert interrupted.value.status == 503
            assert caplog.messages[-1] == f"Preview worker stopped: account=reader-1 sha256={digest} reason=external-kill"
        assert (await request(host, projectors.delayed))[0] == b"example"
    finally:
        await host.close()


@pytest.mark.asyncio
async def test_only_actual_child_failures_log_a_sanitized_account_and_snapshot_digest(projectors, monkeypatch, caplog):
    host = ProjectionHost()
    host.start([projectors.refuse, projectors.echo])
    try:
        await host.wait_ready()
        caplog.clear()
        source = b"private file contents"
        account = "/private/path\nforged-entry"
        account_hash = hashlib.sha256(account.encode()).hexdigest()[:16]
        digest = hashlib.sha256(source).hexdigest()
        for _ in range(2):
            with pytest.raises(ProjectionError):
                await request(host, projectors.refuse, source, fingerprint_raw=b"stale", requester_id=account)
        with pytest.raises(ProjectionError):
            await request(host, projectors.echo, source, preview_limit=3, requester_id="reader-2")
        assert (await request(host, projectors.echo, b"ok"))[0] == b"ok"
        monkeypatch.setattr(projection_host, "MAX_WAITERS", -1)
        with pytest.raises(ProjectionError):
            await request(host, projectors.echo, source)
        assert caplog.messages == [
            f"Preview worker stopped: account=sha256-{account_hash} sha256={digest} reason=projector-refused",
            f"Preview worker stopped: account=reader-2 sha256={digest} reason=output-budget",
        ]
    finally:
        await host.close()
