"""Admission, process ownership and revision-scoped caching for installed file previews."""

import asyncio
import contextlib
import hashlib
import inspect
import json
import logging
import math
import os
import re
import signal
import subprocess
import sys
import threading
import time
from collections import OrderedDict, deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

logger = logging.getLogger(__name__)
WALL_SECONDS = 10
CPU_SECONDS = 8
MEMORY_BYTES = 256 * 1024 * 1024
WAIT_SECONDS = 8
MAX_WAITERS = 8
WINDOW_SECONDS = 60
WORKER_SECONDS = 20
MEMO_BYTES = 16 * 1024 * 1024
MEMO_COUNT = 64
REFUSAL_SECONDS = 600
_WORKER = str(Path(__file__).with_name("projection_worker.py"))
_CHUNK = 64 * 1024


class ProjectionError(Exception):
    def __init__(self, status, retry_after=None):
        super().__init__(status)
        self.status = status
        self.retry_after = retry_after


def projector_reference(function):
    """Only the installed module's own directly importable Python function is portable."""
    module = sys.modules.get(getattr(function, "__module__", None))
    name = getattr(function, "__name__", "")
    if (
        not inspect.isfunction(function)
        or inspect.iscoroutinefunction(function)
        or function.__closure__
        or not name.isidentifier()
        or function.__qualname__ != name
        or module is None
        or module.__name__ == "__main__"
        or not getattr(module, "__file__", None)
        or getattr(module, name, None) is not function
    ):
        raise ValueError("Artifact projector must be an importable module-level synchronous function")
    return module.__name__, name


def _log_account(requester_id):
    if not isinstance(requester_id, str) or not requester_id:
        return "unknown"
    if re.fullmatch(r"[A-Za-z0-9_-]{1,128}", requester_id):
        return requester_id
    return "sha256-" + hashlib.sha256(requester_id.encode("utf-8", errors="surrogatepass")).hexdigest()[:16]


def _invoke(mode, function, raw, source_limit, output_limit, wall, stop, audit=None):
    module, name = projector_reference(function) if function else ("", "")
    origin = os.path.realpath(sys.modules[module].__file__) if function else ""
    paths = json.dumps([os.path.abspath(path) for path in sys.path if isinstance(path, str)])
    memory = 64 * 1024 * 1024 if mode == "self-test" else MEMORY_BYTES
    cpu = 1 if mode == "self-test-cpu" else CPU_SECONDS
    args = [sys.executable, "-I", "-B", "-S", _WORKER, mode, module, name, origin, paths, str(memory), str(cpu), str(source_limit)]
    deadline = time.monotonic() + wall
    header_bytes = 4 if mode == "project" else 0
    wire_limit = output_limit + header_bytes
    try:
        process = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env={}, close_fds=True, start_new_session=os.name == "posix", bufsize=0)
    except OSError:
        raise ProjectionError(501) from None
    reason = None
    try:
        # Pump in both directions. communicate() buffers all stdout and could
        # retain another full source-sized allocation while sending stdin.
        with process.stdin as source, process.stdout as result:
            os.set_blocking(source.fileno(), False)
            os.set_blocking(result.fileno(), False)
            pending = memoryview(raw)
            if not pending:
                source.close()
            output = bytearray()
            eof = False
            while True:
                if stop.is_set():
                    reason = "cancelled"
                    raise ProjectionError(503, 1)
                if time.monotonic() >= deadline:
                    reason = "wall-deadline"
                    raise ProjectionError(503, 1)
                progressed = False
                if not eof:
                    try:
                        chunk = os.read(result.fileno(), min(_CHUNK, wire_limit + 1 - len(output)))
                    except BlockingIOError:
                        pass
                    else:
                        eof = not chunk
                        if chunk:
                            output.extend(chunk)
                            if len(output) > wire_limit:
                                reason = "output-budget"
                                raise ProjectionError(413)
                            progressed = True
                if pending:
                    try:
                        count = os.write(source.fileno(), pending[:_CHUNK])
                    except BlockingIOError:
                        pass
                    except BrokenPipeError:
                        pending = pending[:0]
                    else:
                        pending = pending[count:]
                        progressed |= count > 0
                    if not pending:
                        source.close()
                if eof and process.poll() is not None:
                    break
                if not progressed:
                    stop.wait(0.005)
        status = process.returncode
        if status == 0:
            if header_bytes and (len(output) < header_bytes or int.from_bytes(output[:header_bytes], "big") != len(output) - header_bytes):
                reason = "invalid-output"
                raise ProjectionError(422)
            return bytes(memoryview(output)[header_bytes:])
        if status == 22:
            reason = "source-budget"
            raise ProjectionError(413)
        if status == 20:
            reason = "projector-refused"
            raise ProjectionError(422)
        if os.name == "posix" and status == -signal.SIGXCPU:
            reason = "cpu-limit"
            raise ProjectionError(422)
        if os.name == "posix" and status == -signal.SIGKILL:
            # An external kill (including container memory pressure) need not
            # mean that this particular source is invalid. Allow a later retry.
            reason = "external-kill"
            raise ProjectionError(503, 1)
        reason = "worker-unavailable"
        raise ProjectionError(501)
    except OSError:
        reason = "pipe-error"
        raise ProjectionError(501) from None
    finally:
        if process.returncode is None:
            # Preserve ownership through every exit. The leader remains ours
            # until wait() reaps it, including when pipes have already closed.
            with contextlib.suppress(ProcessLookupError):
                if os.name == "posix":
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
            process.wait()
        if audit is not None and reason is not None:
            account, digest = audit
            logger.warning("Preview worker stopped: account=%s sha256=%s reason=%s", account, digest, reason)


async def _await_owned(future, stop=None):
    """Retain the executor operation and its input until cleanup has finished."""
    try:
        return await asyncio.shield(future)
    except asyncio.CancelledError:
        if stop is not None:
            stop.set()
        while not future.done():
            try:
                await asyncio.shield(future)
            except (Exception, asyncio.CancelledError):
                continue
        if not future.cancelled():
            future.exception()
        raise


class ProjectionHost:
    def __init__(self):
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="artifact-projection")
        self._slot = asyncio.Lock()
        self._requests = set()
        self._startup = None
        self._available = set()
        self._closed = False
        self._usage = deque()
        self._memo = OrderedDict()
        self._memo_bytes = 0

    def start(self, projectors):
        if self._closed or self._startup is not None:
            raise RuntimeError("Projection host already started or closed")
        self._startup = asyncio.create_task(self._prepare(tuple(dict.fromkeys(projectors))))

    async def wait_ready(self):
        if self._startup is not None:
            await asyncio.shield(self._startup)

    async def _run(self, mode, function=None, raw=b"", source_limit=0, output_limit=128, wall=None, audit=None):
        stop = threading.Event()
        future = asyncio.get_running_loop().run_in_executor(self._executor, _invoke, mode, function, raw, source_limit, output_limit, WALL_SECONDS if wall is None else wall, stop, audit)
        return await _await_owned(future, stop)

    async def _prepare(self, projectors):
        if os.name != "posix":
            logger.warning("Preview worker process memory and CPU limits are unavailable on this platform")
        if projectors:
            try:
                await self._run("self-test")
                if os.name == "posix":
                    await self._expect_stop("self-test-cpu", status=422, wall=WALL_SECONDS)
                await self._expect_stop("self-test-deadline", status=503, wall=0.3)
            except (OSError, ValueError, ProjectionError):
                logger.warning("Preview worker self-test failed; installed projections are unavailable")
                return
        for function in projectors:
            try:
                started = time.monotonic()
                measurement = await self._run("load", function)
                address, rss = map(int, measurement.split())
            except (OSError, ProjectionError, ValueError):
                logger.warning("Preview worker could not load %s.%s", function.__module__, function.__name__)
            else:
                self._available.add(function)
                logger.info("Preview worker loaded %s.%s in %.3f seconds (peak address space %d KiB, peak RSS %d KiB)", function.__module__, function.__name__, time.monotonic() - started, address, rss)
        logger.info("Preview worker self-test complete: %d/%d projectors available", len(self._available), len(projectors))

    async def _expect_stop(self, mode, *, status, wall):
        try:
            await self._run(mode, wall=wall)
        except ProjectionError as exc:
            if exc.status == status:
                return
            raise
        raise ProjectionError(501)

    async def close(self):
        self._closed = True
        owned = self._requests.copy()
        if self._startup is not None:
            owned.add(self._startup)
        for task in owned:
            task.cancel()
        if owned:
            await asyncio.gather(*owned, return_exceptions=True)
        self._executor.shutdown(wait=False)
        self._memo.clear()
        self._memo_bytes = 0

    def _cached(self, key):
        if key not in self._memo:
            return None
        content, status, expiry = self._memo[key]
        if status and time.monotonic() >= expiry:
            del self._memo[key]
            return None
        self._memo.move_to_end(key)
        if status:
            raise ProjectionError(status)
        return content

    def _remember(self, key, *, content=b"", status=0):
        self._memo[key] = content, status, time.monotonic() + REFUSAL_SECONDS if status else None
        self._memo_bytes += len(content)
        while len(self._memo) > MEMO_COUNT or self._memo_bytes > MEMO_BYTES:
            _, (removed, _, _) = self._memo.popitem(last=False)
            self._memo_bytes -= len(removed)

    def _remaining(self, now):
        cutoff = now - WINDOW_SECONDS
        while self._usage and self._usage[0][1] <= cutoff:
            self._usage.popleft()
        seconds = WORKER_SECONDS - sum(end - max(start, cutoff) for start, end in self._usage)
        if seconds <= 0:
            raise ProjectionError(503, max(1, math.ceil(self._usage[0][1] - cutoff)))
        return seconds

    async def project(self, *, key, projector, source_max_bytes, preview_max_bytes, fingerprint, read_source, requester_id=None):
        if self._closed or self._startup is None:
            raise ProjectionError(501)
        if not self._startup.done():
            raise ProjectionError(503, 1)
        if projector not in self._available:
            raise ProjectionError(501)
        if len(self._requests) >= MAX_WAITERS + 1:
            raise ProjectionError(503, 1)
        # Count hashing as an admitted request too, before submitting any
        # shared file-pool work. Only the slot holder owns a full snapshot.
        task = asyncio.current_task()
        self._requests.add(task)
        try:
            declaration = (key, projector, source_max_bytes, preview_max_bytes)
            digest, size = await fingerprint()
            if size > source_max_bytes:
                raise ProjectionError(413)
            cached = self._cached((declaration, digest))
            if cached is not None:
                return cached, digest, size
            try:
                async with asyncio.timeout(WAIT_SECONDS):
                    await self._slot.acquire()
            except TimeoutError:
                raise ProjectionError(503, 1) from None
            try:
                cached = self._cached((declaration, digest))
                if cached is not None:
                    return cached, digest, size
                self._remaining(time.monotonic())
                raw = await read_source()
                if len(raw) > source_max_bytes:
                    raise ProjectionError(413)
                sha = await _await_owned(asyncio.get_running_loop().run_in_executor(self._executor, hashlib.sha256, raw))
                digest = sha.hexdigest()
                memo_key = (declaration, digest)
                content = self._cached(memo_key)
                if content is None:
                    started = time.monotonic()
                    wall = min(WALL_SECONDS, self._remaining(started))
                    try:
                        content = await self._run("project", projector, raw, source_max_bytes, preview_max_bytes, wall, (_log_account(requester_id), digest))
                    except ProjectionError as exc:
                        if exc.status in (413, 422):
                            self._remember(memo_key, status=exc.status)
                        raise
                    finally:
                        self._usage.append((started, time.monotonic()))
                    self._remember(memo_key, content=content)
                return content, digest, len(raw)
            finally:
                self._slot.release()
        finally:
            self._requests.discard(task)
