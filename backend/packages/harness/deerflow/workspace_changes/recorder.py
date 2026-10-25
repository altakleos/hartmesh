from __future__ import annotations

import asyncio
import logging
import shutil
import sys
import tempfile
from functools import partial
from pathlib import Path
from typing import Any

from deerflow.config import get_paths
from deerflow.utils.file_io import await_drained, run_file_io

from .diff import compare_snapshots, get_changed_paths
from .scanner import scan_workspace_roots
from .types import (
    WORKSPACE_CHANGES_EVENT_CATEGORY,
    WORKSPACE_CHANGES_EVENT_TYPE,
    WORKSPACE_CHANGES_METADATA_KEY,
    WorkspaceChangeLimits,
    WorkspaceRoot,
    WorkspaceSnapshot,
)

logger = logging.getLogger(__name__)

_SNAPSHOT_CACHES: dict[str, _SnapshotCache] = {}


class _SnapshotCache:
    """Retain an exact temporary cache without extending graph lifetime."""

    def __init__(self, owner):
        self.owner = owner
        self.path = None
        self._close_task = None
        owner.register_resource(self)

    def prepared(self, path):
        self.path = path
        if path is not None:
            _SNAPSHOT_CACHES[str(path)] = self

    def retire(self):
        # Unlike an idle SQL iterator, the cache may still serve an admitted
        # scan. The owner joins those operations before invoking drain.
        pass

    async def drain(self):
        from deerflow.runtime.runs.capture_io import CaptureIOCleanupError

        if self._close_task is None:

            async def close():
                if self.path is not None:

                    def remove():
                        try:
                            shutil.rmtree(self.path)
                        except FileNotFoundError:
                            pass

                    failed = False
                    try:
                        await run_file_io(remove)
                    except Exception:
                        failed = True
                    if failed:
                        raise CaptureIOCleanupError() from None
                    _SNAPSHOT_CACHES.pop(str(self.path), None)
                self.owner.unregister_resource(self)

            self._close_task = asyncio.create_task(close(), name="snapshot-cache-close")
        await await_drained(asyncio.shield(self._close_task))


async def _settle_failed_snapshot_cache(cache):
    """Preserve the primary outcome; failed close stays registered for host drain."""
    from deerflow.runtime.runs.capture_io import CaptureIOCleanupError

    try:
        await cache.drain()
    except CaptureIOCleanupError:
        pass


def build_thread_workspace_roots(thread_id: str, *, user_id: str | None = None) -> list[WorkspaceRoot]:
    paths = get_paths()
    return [
        WorkspaceRoot(
            name="workspace",
            host_path=paths.sandbox_work_dir(thread_id, user_id=user_id),
            virtual_prefix="/mnt/user-data/workspace",
        ),
        WorkspaceRoot(
            name="outputs",
            host_path=paths.sandbox_outputs_dir(thread_id, user_id=user_id),
            virtual_prefix="/mnt/user-data/outputs",
        ),
    ]


def _prepare_capture(thread_id: str, *, user_id: str | None, include_text: bool, outputs_only: bool = False) -> tuple[list[WorkspaceRoot], Path | None]:
    # Worker thread: resolving the sandbox roots hits the filesystem, and mkdtemp
    # creates the text cache directory — both blocking IO that must stay off the
    # event loop.
    if outputs_only:
        roots = [WorkspaceRoot(name="outputs", host_path=get_paths().sandbox_outputs_dir(thread_id, user_id=user_id), virtual_prefix="/mnt/user-data/outputs")]
    else:
        roots = build_thread_workspace_roots(thread_id, user_id=user_id)
    text_cache_dir = Path(tempfile.mkdtemp(prefix="deerflow-workspace-changes-")) if include_text else None
    return roots, text_cache_dir


async def _remove_text_cache_dir(text_cache_dir: str | Path) -> None:
    """Remove a snapshot's text cache off the event loop.

    Best-effort by contract: every caller is a failure or teardown path, so a
    cleanup error must never replace the exception or result already in flight.
    """
    try:
        await asyncio.to_thread(shutil.rmtree, text_cache_dir, ignore_errors=True)
    except Exception:
        logger.warning("Failed to remove workspace text cache %s", text_cache_dir, exc_info=True)


async def _reclaim_prepare_and_cleanup(prepare: asyncio.Future[tuple[list[WorkspaceRoot], Path | None]]) -> None:
    """Await a cancelled prepare handoff and remove any dir it created.

    Owned by its own task so that caller cancellation during reclaim can interrupt
    the *await* but never abandon a just-created text cache dir. Best-effort,
    mirroring `_remove_text_cache_dir`.
    """
    try:
        _, orphaned = await prepare
    except Exception:
        return  # prepare failed before creating a dir; nothing to reclaim
    if orphaned is not None:
        await _remove_text_cache_dir(orphaned)


def _consume_cancelled_scan_outcome(scan: asyncio.Future[WorkspaceSnapshot], *, thread_id: str) -> None:
    """Consume a completed scan and retain diagnostics after caller cancellation."""
    try:
        scan.result()
    except asyncio.CancelledError:
        return
    except Exception:
        logger.warning(
            "Workspace scan failed after snapshot cancellation for thread %s",
            thread_id,
            exc_info=True,
        )


async def _drain_scan_and_cleanup(
    scan: asyncio.Future[WorkspaceSnapshot],
    text_cache_dir: Path,
    *,
    thread_id: str,
) -> None:
    """Let a cancelled scan finish before removing the cache it may still use."""
    if not scan.done():
        logger.info(
            "Waiting for cancelled workspace snapshot scan to finish before text-cache cleanup for thread %s",
            thread_id,
        )

    while not scan.done():
        try:
            await asyncio.shield(scan)
        except asyncio.CancelledError:
            pass
        except Exception:
            break

    # Cancellation remains the caller-visible outcome, but consume any late scan
    # failure so the drained task cannot emit an un-retrieved exception warning.
    _consume_cancelled_scan_outcome(scan, thread_id=thread_id)

    cleanup = asyncio.create_task(_remove_text_cache_dir(text_cache_dir))
    while not cleanup.done():
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            pass


async def capture_workspace_snapshot(
    thread_id: str,
    *,
    user_id: str | None = None,
    limits: WorkspaceChangeLimits | None = None,
    include_text: bool = True,
    extra_excluded_dir_names: frozenset[str] | None = None,
    outputs_only: bool = False,
) -> WorkspaceSnapshot:
    from deerflow.runtime.runs.capture_io import current_capture_io_owner, run_capture_io
    from deerflow.spaces.chat_paths import ChatStoragePaths

    owner = current_capture_io_owner()
    if owner is not None:
        owner.check_open()
    paths = get_paths()
    if isinstance(paths, ChatStoragePaths):
        from deerflow.spaces.chat_snapshots import capture_chat_snapshot

        return await run_capture_io(lambda: capture_chat_snapshot(paths, thread_id, user_id=user_id, limits=limits, include_text=include_text, outputs_only=outputs_only, extra_excluded_dir_names=extra_excluded_dir_names))
    # `_prepare_capture` creates the text cache dir inside the worker, so the
    # handoff must be cancellation-safe: if the run is cancelled after mkdtemp
    # but before we receive the path, the shielded worker still finishes and we
    # reclaim its result to remove the orphaned dir before re-raising.
    cache = _SnapshotCache(owner) if owner is not None and include_text else None

    async def prepare_capture():
        result = await asyncio.to_thread(_prepare_capture, thread_id, user_id=user_id, include_text=include_text, outputs_only=outputs_only)
        if cache is not None:
            cache.prepared(result[1])
        return result

    prepare = asyncio.ensure_future(run_capture_io(prepare_capture))
    try:
        roots, text_cache_dir = await asyncio.shield(prepare)
    except asyncio.CancelledError:
        # `prepare` is shielded, so it keeps running and may still create the dir
        # after this cancel. Own the reclaim+remove in a task the caller cannot
        # abandon: a repeat cancel can interrupt our await but not the task, so we
        # drain repeated cancellation until cleanup finishes, then restore it. A
        # second `shield()` on the await alone would let a re-cancel skip the
        # reclaim and orphan the dir.
        if cache is None:
            cleanup = asyncio.ensure_future(_reclaim_prepare_and_cleanup(prepare))
        else:

            async def reclaim_owned_prepare():
                try:
                    await prepare
                except Exception:
                    pass
                await _settle_failed_snapshot_cache(cache)

            cleanup = asyncio.ensure_future(reclaim_owned_prepare())
        while not cleanup.done():
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                pass
        raise

    scan = asyncio.ensure_future(
        run_capture_io(
            lambda: asyncio.to_thread(
                scan_workspace_roots,
                roots,
                limits=limits,
                include_text=include_text,
                text_cache_dir=text_cache_dir,
                extra_excluded_dir_names=extra_excluded_dir_names,
            )
        )
    )
    try:
        return await await_drained(scan) if owner is not None else await asyncio.shield(scan)
    except asyncio.CancelledError:
        if cache is not None:
            await _settle_failed_snapshot_cache(cache)
            raise
        # A metadata-only scan has no cache resource to protect. It still runs in
        # the worker after caller cancellation, so retain ownership only long
        # enough to consume/log its eventual outcome instead of delaying the
        # cancellation until a full workspace scan finishes.
        if text_cache_dir is None:
            scan.add_done_callback(partial(_consume_cancelled_scan_outcome, thread_id=thread_id))
            raise

        # Text capture is different: the worker may still read/write the cache,
        # so deleting it immediately would race the scan. Keep the cache alive
        # until the worker drains, then remove it before propagating cancellation.
        # Repeated cancellation must not abandon either phase.
        await _drain_scan_and_cleanup(scan, text_cache_dir, thread_id=thread_id)
        raise
    except Exception:
        if cache is not None:
            await _settle_failed_snapshot_cache(cache)
        elif text_cache_dir is not None:
            await _remove_text_cache_dir(text_cache_dir)
        raise


async def capture_output_snapshot(
    thread_id: str,
    *,
    user_id: str | None = None,
    limits: WorkspaceChangeLimits | None = None,
    extra_excluded_dir_names: frozenset[str] | None = None,
) -> WorkspaceSnapshot:
    """Capture delivery evidence with its own budget; never scan workspace."""
    return await capture_workspace_snapshot(
        thread_id,
        user_id=user_id,
        limits=limits,
        include_text=False,
        outputs_only=True,
        extra_excluded_dir_names=extra_excluded_dir_names,
    )


async def record_workspace_changes(
    event_store: Any,
    thread_id: str,
    run_id: str,
    before: WorkspaceSnapshot,
    *,
    user_id: str | None = None,
    limits: WorkspaceChangeLimits | None = None,
    extra_excluded_dir_names: frozenset[str] | None = None,
) -> dict | None:
    from deerflow.runtime.runs.capture_io import CaptureIOCleanupError, run_capture_io

    try:
        from deerflow.spaces.chat_paths import ChatStoragePaths

        paths = get_paths()
        if isinstance(paths, ChatStoragePaths):
            from deerflow.spaces.chat_snapshots import capture_chat_snapshot

            after_metadata = await run_capture_io(lambda: capture_chat_snapshot(paths, thread_id, user_id=user_id, limits=limits, include_text=False, extra_excluded_dir_names=extra_excluded_dir_names))
            changed_paths = get_changed_paths(before, after_metadata)
            after = await run_capture_io(lambda: capture_chat_snapshot(paths, thread_id, user_id=user_id, limits=limits, text_paths=changed_paths, extra_excluded_dir_names=extra_excluded_dir_names))
        else:
            roots = await run_capture_io(lambda: asyncio.to_thread(build_thread_workspace_roots, thread_id, user_id=user_id))
            after_metadata = await run_capture_io(lambda: asyncio.to_thread(scan_workspace_roots, roots, limits=limits, include_text=False, extra_excluded_dir_names=extra_excluded_dir_names))
            changed_paths = get_changed_paths(before, after_metadata)
            after = await run_capture_io(lambda: asyncio.to_thread(scan_workspace_roots, roots, limits=limits, include_text=True, text_paths=changed_paths, extra_excluded_dir_names=extra_excluded_dir_names))
        result = await run_capture_io(lambda: run_file_io(compare_snapshots, before, after, limits=limits))
        if not result.has_changes():
            return None

        payload = result.to_dict()
        summary = result.summary
        changed_file_count = summary.created + summary.modified + summary.deleted + summary.symlink_created
        content = f"{changed_file_count} file{'s' if changed_file_count != 1 else ''} changed +{summary.additions} -{summary.deletions}"
        return await run_capture_io(
            lambda: event_store.put(
                thread_id=thread_id,
                run_id=run_id,
                event_type=WORKSPACE_CHANGES_EVENT_TYPE,
                category=WORKSPACE_CHANGES_EVENT_CATEGORY,
                content=content,
                metadata={WORKSPACE_CHANGES_METADATA_KEY: payload},
            )
        )
    finally:
        primary = sys.exc_info()[1]
        try:
            await _cleanup_snapshot_text_cache(before)
        except CaptureIOCleanupError:
            if primary is None:
                raise


async def _cleanup_snapshot_text_cache(snapshot: WorkspaceSnapshot) -> None:
    if snapshot.text_cache_dir:
        cache = _SNAPSHOT_CACHES.get(str(snapshot.text_cache_dir))
        if cache is not None:
            await cache.drain()
        else:
            await _remove_text_cache_dir(snapshot.text_cache_dir)
