"""Run-scoped host leases; controlled refreshes are not storage qualifications."""

import asyncio
import importlib
from copy import copy, deepcopy
from dataclasses import replace

import pytest

from deerflow.config.paths import Paths, paths_scope
from deerflow.runtime.runs.capture_io import CaptureIOClosed, CaptureIOOwner, capture_io_scope
from deerflow.runtime.runs.output import OutputEvidenceError


@pytest.fixture
def api():
    return importlib.import_module("deerflow.runtime.runs.output_view")


@pytest.mark.asyncio
async def test_issued_lease_confirms_only_original_scoped_request(api, tmp_path):
    owner = CaptureIOOwner()
    paths = Paths(tmp_path)
    prepared = object()
    calls = []
    with paths_scope(paths), capture_io_scope(owner):
        request = api.OutputViewRequest("owner", "thread", "run", paths, prepared, owner)

        async def refresh():
            calls.append("refresh")

        async def issue(actual):
            assert actual is request
            return api.NoAdmittedOutputViewLease(actual, refresh=refresh)

        qualifier = api.OutputViewQualifier(issue)
        lease = await qualifier(request)
        assert calls == []
        result = await lease.confirm(request)
        assert result.kind == "no_admitted_output_view"
        assert result.scope.kind == "managed_output_routing"
        assert result.scope.virtual_root == "/mnt/user-data/outputs"
        assert calls == ["refresh"]
        with pytest.raises(OutputEvidenceError):
            await lease.confirm(replace(request))
        assert calls == ["refresh"]
        lease.retire()
        with pytest.raises(OutputEvidenceError):
            await lease.confirm(request)
    owner.retire()
    await owner.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["paths", "owner", "retired", "no_scope"])
async def test_lease_refuses_foreign_or_closed_context_before_refresh(api, tmp_path, change):
    owner = CaptureIOOwner()
    paths = Paths(tmp_path)
    calls = []

    async def refresh():
        calls.append("refresh")

    with paths_scope(paths), capture_io_scope(owner):
        request = api.OutputViewRequest("owner", "thread", "run", paths, object(), owner)
        lease = api.NoAdmittedOutputViewLease(request, refresh=refresh)
    active_paths = Paths(tmp_path / "other") if change == "paths" else paths
    active_owner = CaptureIOOwner() if change == "owner" else owner
    if change == "retired":
        owner.retire()
    with paths_scope(active_paths), capture_io_scope(None if change == "no_scope" else active_owner):
        with pytest.raises((OutputEvidenceError, CaptureIOClosed)):
            await lease.confirm(request)
    assert calls == []
    lease.retire()
    active_owner.retire()
    owner.retire()
    await active_owner.drain()
    await owner.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", [None, False, {}, "no_admitted_output_view"])
async def test_qualifier_never_accepts_wire_values_as_leases(api, tmp_path, bad):
    owner = CaptureIOOwner()
    with paths_scope(Paths(tmp_path)) as _unused, capture_io_scope(owner):
        from deerflow.config.paths import peek_scoped_paths

        request = api.OutputViewRequest("owner", "thread", "run", peek_scoped_paths(), object(), owner)

        async def issue(_request):
            return bad

        qualifier = api.OutputViewQualifier(issue)
        if bad is None:
            assert await qualifier(request) is None
        else:
            with pytest.raises(OutputEvidenceError):
                await qualifier(request)
    owner.retire()
    await owner.drain()


@pytest.mark.asyncio
async def test_started_refresh_settles_through_repeated_cancel_without_positive_result(api, tmp_path):
    owner = CaptureIOOwner()
    entered, release, settled = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def refresh():
        entered.set()
        await release.wait()
        settled.set()

    with paths_scope(Paths(tmp_path)), capture_io_scope(owner):
        from deerflow.config.paths import peek_scoped_paths

        request = api.OutputViewRequest("owner", "thread", "run", peek_scoped_paths(), object(), owner)
        lease = api.NoAdmittedOutputViewLease(request, refresh=refresh)
        task = asyncio.create_task(lease.confirm(request))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            task.cancel()
            assert not (await asyncio.wait({task}, timeout=0.02))[0]
            task.cancel()
            assert not (await asyncio.wait({task}, timeout=0.02))[0]
            lease.retire()
            owner.retire()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert settled.is_set()
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
            lease.retire()
            owner.retire()
            await owner.drain()


@pytest.mark.asyncio
async def test_retirement_during_refresh_cannot_return_attestation(api, tmp_path):
    owner = CaptureIOOwner()
    entered, release = asyncio.Event(), asyncio.Event()

    async def refresh():
        entered.set()
        await release.wait()

    with paths_scope(Paths(tmp_path)), capture_io_scope(owner):
        from deerflow.config.paths import peek_scoped_paths

        request = api.OutputViewRequest("owner", "thread", "run", peek_scoped_paths(), object(), owner)
        lease = api.NoAdmittedOutputViewLease(request, refresh=refresh)
        task = asyncio.create_task(lease.confirm(request))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            owner.retire()
            release.set()
            with pytest.raises(CaptureIOClosed):
                await task
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
            lease.retire()
            owner.retire()
            await owner.drain()


@pytest.mark.asyncio
async def test_copied_lease_cannot_reopen_retired_lifetime(api, tmp_path):
    from deerflow.config.paths import peek_scoped_paths

    owner = CaptureIOOwner()

    async def refresh():
        pass

    with paths_scope(Paths(tmp_path)), capture_io_scope(owner):
        request = api.OutputViewRequest("owner", "thread", "run", peek_scoped_paths(), object(), owner)
        lease = api.NoAdmittedOutputViewLease(request, refresh=refresh)
        copied = copy(lease)
        lease.retire()
        try:
            with pytest.raises(OutputEvidenceError):
                await copied.confirm(request)
        finally:
            copied.retire()
            owner.retire()
            await owner.drain()
        assert deepcopy(lease) is lease
