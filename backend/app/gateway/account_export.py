"""Download all my data: one person's own work, prepared on the data disk, downloaded, then deleted.

A person starts it from their own browser session, and it only ever reads the
caller's own data (``app.gateway.routers.account_export``): the conversations
the thread store records under their id, the files under their own user
directory, and their memory, schedules and custom agents. There is no user-id
parameter and no administrator path, so no one can export another person's
work.

The archive::

    README.md                             what is in it, in words: each conversation's title and folder, what was left out and why
    manifest.json                         who, when, which release, and every file with its size and SHA-256
    conversations/<id>/transcript.md      the conversation as the page shows it (app.gateway.transcript)
    conversations/<id>/transcript.json
    conversations/<id>/files/uploads/...  everything in the conversation's user-data directories,
    conversations/<id>/files/outputs/...  presented or not, except the reserved directories
    conversations/<id>/files/workspace/...
    my-files/...                          the person's own files, kept across conversations
    my-skills/...                         the skills the person made
    memory.json                           what GET /api/memory/export returns
    scheduled-tasks.json                  each schedule's definition
    agents.json                           each custom agent's definition, where the feature is on
    agents/<name>/memory.json             what each custom agent remembers

It holds no credential: no token, provider key or connection secret is in any
of these, and nothing an administrator configured for the whole company. The
reserved directories (tool-result spill, browser frames, MCP servers' own
state) are process state, not the person's work, and are left out.

It is built into a private directory under ``Paths.exports_dir()`` after a
free-space check, split into numbered parts past ``part_bytes``, and deleted
``REDOWNLOAD_SECONDS`` after its last part was downloaded, when nothing has
been downloaded for ``expires_after_seconds``, or when the Gateway stops or
starts. A file that cannot be archived safely -- a link, a hard link, a name
Windows cannot hold, one that changed, vanished or could not be read while
it was copied -- is left out and named under the manifest's ``skipped``, so
the person can tell the archive is complete.

The jobs live in this process, so the service runs only where one Gateway
process serves every request (:func:`runs_in_this_process`).

The logs carry counts and sizes, never a title, a file name, a path below the
user directory or any content: one export walks every file a person owns, and
naming them would write their whole file list in one pass. For the same
reason no exception is logged with its message.
"""

from __future__ import annotations

import asyncio
import contextlib
import errno
import hashlib
import json
import logging
import os
import secrets
import shutil
import stat
import threading
import time
import tomllib
import unicodedata
import zipfile
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import cache
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from app.gateway import transcript
from app.gateway.artifact_archive import ArtifactArchiveError, copy_file, reserved_dir_names, unsafe_entry_parts
from app.gateway.routers.agents import owned_agent_documents
from app.gateway.routers.memory import memory_export_document
from deerflow.config.account_export_config import AccountExportConfig
from deerflow.config.paths import Paths
from deerflow.constants import MCP_INTERNAL_DIRNAME
from deerflow.runtime.owner_holdings import Ended

logger = logging.getLogger(__name__)

MANIFEST_NAME = "manifest.json"
README_NAME = "README.md"
FORMAT = "account-export"
FORMAT_VERSION = 1

#: A conversation's directories that are the person's work, in archive order.
USER_DATA_AREAS = ("uploads", "outputs", "workspace")

#: What a schedule's definition is; the rest of its row is the scheduler's own bookkeeping.
SCHEDULE_FIELDS = ("id", "title", "prompt", "schedule_type", "schedule_spec", "timezone", "status", "context_mode", "assistant_id", "thread_id", "overlap_policy", "created_at", "updated_at", "next_run_at", "last_run_at")

#: How long an export stays once every part has been downloaded, for one that did not arrive whole.
REDOWNLOAD_SECONDS = 600

#: Every thread in one read: pages ordered by last update drop or repeat a thread that changes between them.
_ALL_THREADS = 2**31 - 1
_TEXT_MEDIA_SUFFIXES = (".md", ".json")

#: Why a file was left out, as the README says it.
_REASONS = {
    "link": "a link to somewhere else",
    "hard_link": "a second name for another file",
    "not_a_file": "not a regular file",
    "unsafe_name": "a name some computers cannot hold",
    "name_collision": "a name that differs from another only by letter case",
    "changed": "it changed while it was being copied",
    "vanished": "it was deleted while the export was prepared",
    "unreadable": "it could not be read",
    "too_large": "larger than this deployment's disk",
}


class ExportRefused(Exception):
    """An export that cannot be prepared; ``code`` is what the page says."""

    code = "failed"

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


class ExportBusy(ExportRefused):
    code = "busy"


class NoSpace(ExportRefused):
    code = "no_space"


def _no_space() -> NoSpace:
    return NoSpace("there is not enough free space to prepare the export; ask your administrator")


def runs_in_this_process(*, multi_gateway: bool) -> bool:
    """Whether exports can run here: each is kept by the process that prepares it.

    With more than one Gateway process (a multi-Gateway profile, or
    ``GATEWAY_WORKERS`` above one), a status or download request could reach
    one that never heard of the export, and a process that starts would
    delete what another prepared.
    """
    try:
        workers = int(os.environ.get("GATEWAY_WORKERS", "1") or 1)
    except ValueError:
        workers = 1
    return not multi_gateway and workers <= 1


@cache
def release() -> str:
    """The release this Gateway runs, from the backend's own version source."""
    try:
        with (Path(__file__).resolve().parents[2] / "pyproject.toml").open("rb") as source:
            return str(tomllib.load(source)["project"]["version"])
    except (OSError, KeyError, tomllib.TOMLDecodeError):
        return "unknown"


@dataclass
class Progress:
    conversations_total: int = 0
    conversations_done: int = 0
    files_total: int = 0
    files_done: int = 0
    bytes_total: int = 0
    bytes_done: int = 0


@dataclass
class ExportJob:
    """One person's export, from the request that started it until its record expires."""

    user_id: str
    directory: Path
    started_at: datetime
    state: str = "building"
    progress: Progress = field(default_factory=Progress)
    parts: list[Path] = field(default_factory=list)
    part_sizes: list[int] = field(default_factory=list)
    sent: set[int] = field(default_factory=set)
    skipped: int = 0
    error: str | None = None
    detail: str | None = None
    expires_at: datetime | None = None
    task: asyncio.Task | None = None
    cancel: threading.Event = field(default_factory=threading.Event)
    expiry: asyncio.TimerHandle | None = None
    #: Downloads of this export's parts still streaming; it never expires under one.
    streams: int = 0
    stage: str = "listing"

    def document(self) -> dict[str, Any]:
        """What the page shows: the state, how far it got, and each part to download."""
        document: dict[str, Any] = {
            "state": self.state,
            "started_at": self.started_at.isoformat(),
            "progress": vars(self.progress).copy(),
            "parts": [{"number": number, "size": size, "downloaded": number in self.sent} for number, size in enumerate(self.part_sizes, start=1)],
            "skipped": self.skipped,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
        }
        if self.error:
            document["error"] = {"code": self.error, "detail": self.detail}
        return document


@dataclass(frozen=True)
class PartDownload:
    """One part on its way out; ``AccountExportService.close_part`` ends it."""

    job: ExportJob
    number: int
    path: Path
    size: int


@dataclass(frozen=True, slots=True)
class _Found:
    """A file found under the person's own directories, as ``copy_file`` needs it; kept small, as there can be many."""

    path: str
    entry: str
    identity: tuple[int, int]
    size: int
    #: Shared by every file of one directory.
    components: tuple[tuple[Path, int, int], ...]


@dataclass
class _Plan:
    files: list[_Found] = field(default_factory=list)
    skipped: list[dict[str, str]] = field(default_factory=list)
    thread_folders: list[str] = field(default_factory=list)


def _is_link(path: Path, metadata: os.stat_result) -> bool:
    return stat.S_ISLNK(metadata.st_mode) or path.is_junction()


def _unsafe_segment(name: str) -> bool:
    """Whether a name used as one folder of the archive -- a thread id, an agent's name -- cannot be one."""
    return "/" in name or "\\" in name or unsafe_entry_parts([name], frozenset())


def _walk(root: Path, prefix: str, ancestors: tuple[tuple[Path, int, int], ...], plan: _Plan, seen: set[str], reserved: frozenset[str], cancel: threading.Event) -> None:
    """Every regular file under ``root`` into ``plan``, never following a link; what cannot go is named under ``skipped``.

    Something deleted while the walk runs is no longer the person's work and
    is passed over; a directory that cannot be read is named.
    """
    try:
        metadata = os.lstat(root)
    except FileNotFoundError:
        return
    except OSError:
        plan.skipped.append({"path": prefix, "reason": "unreadable"})
        return
    if _is_link(root, metadata) or not stat.S_ISDIR(metadata.st_mode):
        plan.skipped.append({"path": prefix, "reason": "link" if _is_link(root, metadata) else "not_a_file"})
        return
    pending: list[tuple[Path, list[str], tuple[tuple[Path, int, int], ...]]] = [(root, [], (*ancestors, (root, metadata.st_dev, metadata.st_ino)))]
    while pending:
        if cancel.is_set():
            raise asyncio.CancelledError
        directory, parts, components = pending.pop()
        try:
            with os.scandir(directory) as scan:
                children = sorted(scan, key=lambda child: child.name)
        except FileNotFoundError:
            continue
        except OSError:
            plan.skipped.append({"path": f"{prefix}/{'/'.join(parts)}" if parts else prefix, "reason": "unreadable"})
            continue
        subdirectories: list[tuple[Path, list[str], tuple[tuple[Path, int, int], ...]]] = []
        for child in children:
            child_parts = [*parts, child.name]
            entry = f"{prefix}/{'/'.join(child_parts)}"
            path = Path(child.path)
            try:
                child_metadata = os.lstat(path)
            except FileNotFoundError:
                continue
            except OSError:
                plan.skipped.append({"path": entry, "reason": "unreadable"})
                continue
            if _is_link(path, child_metadata):
                plan.skipped.append({"path": entry, "reason": "link"})
            elif stat.S_ISDIR(child_metadata.st_mode):
                if child.name.casefold() in reserved:
                    continue
                subdirectories.append((path, child_parts, (*components, (path, child_metadata.st_dev, child_metadata.st_ino))))
            elif not stat.S_ISREG(child_metadata.st_mode):
                plan.skipped.append({"path": entry, "reason": "not_a_file"})
            elif unsafe_entry_parts(child_parts, reserved):
                plan.skipped.append({"path": entry, "reason": "unsafe_name"})
            elif child_metadata.st_nlink != 1:
                plan.skipped.append({"path": entry, "reason": "hard_link"})
            else:
                # Two names one case-insensitive disk would make one file.
                key = unicodedata.normalize("NFC", entry).casefold()
                if key in seen:
                    plan.skipped.append({"path": entry, "reason": "name_collision"})
                    continue
                seen.add(key)
                plan.files.append(_Found(path=child.path, entry=entry, identity=(child_metadata.st_dev, child_metadata.st_ino), size=child_metadata.st_size, components=components))
        pending.extend(reversed(subdirectories))


def _disk_full(exc: BaseException) -> bool:
    """Whether the data disk filled, whichever write found it: a small entry's write surfaces as a plain ``OSError``."""
    while exc is not None:
        if isinstance(exc, OSError) and exc.errno in (errno.ENOSPC, errno.EDQUOT):
            return True
        exc = exc.__cause__
    return False


def _left_out(exc: ArtifactArchiveError) -> str | None:
    """Why a file that could not be copied is left out, or ``None`` when the export itself cannot go on."""
    if exc.code != "artifact_changed":
        return None
    if _disk_full(exc):
        raise _no_space() from None
    cause = exc.__cause__
    if isinstance(cause, FileNotFoundError):
        return "vanished"
    if isinstance(cause, PermissionError):
        return "unreadable"
    return "changed"


class _Parts:
    """The archive, written as numbered ZIP parts no larger than the ceiling unless one file alone is."""

    def __init__(self, directory: Path, ceiling: int) -> None:
        self._directory = directory
        self._ceiling = ceiling
        self._archive: zipfile.ZipFile | None = None
        self._written = 0
        self.paths: list[Path] = []

    @property
    def number(self) -> int:
        return len(self.paths)

    def _room(self, size: int) -> None:
        if self._archive is not None and self._written and self._written + size > self._ceiling:
            self._close_current()
        if self._archive is None:
            path = self._directory / f"part-{len(self.paths) + 1:03d}.zip"
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            self._archive = zipfile.ZipFile(os.fdopen(descriptor, "wb"), "w", zipfile.ZIP_STORED, allowZip64=True)
            self._written = 0
            self.paths.append(path)

    def add_bytes(self, entry: str, data: bytes) -> dict[str, Any]:
        self._room(len(data))
        assert self._archive is not None
        info = zipfile.ZipInfo(entry, date_time=time.gmtime()[:6])
        info.compress_type = zipfile.ZIP_DEFLATED if entry.endswith(_TEXT_MEDIA_SUFFIXES) else zipfile.ZIP_STORED
        info.create_system = 0
        self._archive.writestr(info, data)
        self._written += len(data)
        return {"path": entry, "size": len(data), "sha256": hashlib.sha256(data).hexdigest(), "part": self.number}

    def add_file(self, found: _Found, cancel: threading.Event) -> dict[str, Any]:
        self._room(found.size)
        assert self._archive is not None
        copied = copy_file(self._archive, Path(found.path), found.entry, identity=found.identity, components=found.components, cancel_event=cancel)
        self._written += copied.size
        return {"path": copied.entry, "size": copied.size, "sha256": copied.sha256, "part": self.number}

    def add_last(self, entry: str, render: Callable[[int], bytes]) -> None:
        """Write the entry that names how many parts there are, rendered for the part it lands in."""
        data = render(max(self.number, 1))
        if self._archive is None or (self._written and self._written + len(data) > self._ceiling):
            data = render(self.number + 1)
        self.add_bytes(entry, data)

    def _close_current(self) -> None:
        if self._archive is not None:
            archive, self._archive = self._archive, None
            handle = archive.fp
            archive.close()
            if handle is not None:
                handle.close()

    def close(self) -> None:
        self._close_current()

    def abandon(self) -> None:
        """Let go of the open part without finishing it: the export is being thrown away."""
        if self._archive is not None:
            archive, self._archive = self._archive, None
            handle, archive.fp = archive.fp, None
            if handle is not None:
                with contextlib.suppress(OSError):
                    handle.close()


def _json(document: Any) -> bytes:
    return json.dumps(document, ensure_ascii=False, indent=2, default=str).encode("utf-8")


def _gone_already(function: Any, path: str, exc: BaseException) -> None:
    if not isinstance(exc, FileNotFoundError):
        raise exc


def _remove(path: Path) -> None:
    """Delete an export's files: a part, or a job's whole directory. What cannot be deleted now goes at the next start."""
    with contextlib.suppress(OSError):
        if path.is_dir() and not path.is_symlink():
            # A part deleted alongside, once downloaded, is no reason to stop.
            shutil.rmtree(path, onexc=_gone_already)
        else:
            path.unlink()


def _markdown_text(text: str) -> str:
    """A title or a path as plain text in Markdown: one line, its markup characters escaped."""
    flat = " ".join(str(text).split())
    return "".join(f"\\{char}" if char in "\\`*_[]<>#|!" else char for char in flat)


def _date(value: str | None) -> str:
    try:
        return datetime.fromisoformat(str(value)).astimezone(UTC).strftime("%Y-%m-%d") if value else "unknown date"
    except ValueError:
        return "unknown date"


def _size(size: int) -> str:
    for unit in ("bytes", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size} {unit}" if unit == "bytes" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} GB"


def _readme(manifest: dict[str, Any], *, own_files: int, own_bytes: int, has: dict[str, bool]) -> bytes:
    """What the archive holds, for a person rather than a program: rendered from the manifest."""
    exported = datetime.fromisoformat(manifest["exported_at"]).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "# Your data",
        "",
        f"Exported on {exported} for {_markdown_text(manifest['person']['email'] or manifest['person']['id'])}.",
        "",
        f"This download holds your own work: {len(manifest['conversations'])} conversations and {own_files} of your files ({_size(own_bytes)}). It holds nothing of anyone else's, and no password, key or connection setting.",
        "",
        "If it came as more than one part, unzip every part into the same folder.",
        "`manifest.json` lists every file with its size and SHA-256 checksum, so a program can check the download is whole.",
        "",
        "## What is where",
        "",
        "- `conversations/<folder>/transcript.md` and `transcript.json`: each conversation as you saw it.",
        "- `conversations/<folder>/files/`: the files of that conversation, what you uploaded (`uploads`), what the assistant made (`outputs`) and its working folder (`workspace`).",
        "- `my-files/`: the files you kept across conversations.",
        "- `my-skills/`: the skills you made.",
    ]
    lines.append("- `memory.json`: what the assistant remembers about you." if has["memory"] else "- There is no `memory.json`: this deployment keeps no memory document to download.")
    lines.append("- `scheduled-tasks.json`: your scheduled tasks." if has["schedules"] else "- There is no `scheduled-tasks.json`: scheduled tasks are not kept on this deployment.")
    lines.append("- `agents.json` and `agents/<name>/memory.json`: your custom agents and what each remembers." if has["agents"] else "- There is no `agents.json`: custom agents are turned off on this deployment.")
    lines += ["", "## Conversations", ""]
    if not manifest["conversations"]:
        lines.append("You have no conversations.")
    for conversation in manifest["conversations"]:
        count = conversation["message_count"]
        messages = "transcript could not be read" if count is None else f"{count} messages"
        lines.append(f"- {_markdown_text(conversation['title'])}: {_date(conversation['created_at'])}, {messages}, in `conversations/{conversation['id']}/`")
    if manifest["folders_without_conversation"]:
        lines += ["", "These folders hold files of conversations that are no longer listed:", ""]
        lines += [f"- `conversations/{folder}/`" for folder in manifest["folders_without_conversation"]]
    lines += ["", "## Left out", ""]
    if not manifest["skipped"]:
        lines.append("Nothing was left out.")
    else:
        lines += ["These could not be included safely:", ""]
        lines += [f"- {_markdown_text(item['path'])}: {_REASONS.get(item['reason'], item['reason'])}" for item in manifest["skipped"]]
    lines += ["", "Working folders the assistant's tools keep for themselves are never included: they are not your work.", ""]
    return "\n".join(lines).encode("utf-8")


def remove_left_exports(paths: Paths) -> None:
    """Delete what a Gateway that stopped mid-export left: nothing may keep it. Never stops a Gateway from starting."""
    root = paths.exports_dir()
    try:
        if not root.is_dir() or root.is_symlink():
            return
        left = list(root.iterdir())
    except OSError as exc:
        logger.warning("Could not look for account exports an earlier Gateway left: %s", type(exc).__name__)
        return
    for child in left:
        _remove(child)
    if left:
        logger.info("Removed %d account exports an earlier Gateway left", len(left))


class AccountExportService:
    """Every person's export on this Gateway: at most one each, a few at once, none left behind."""

    def __init__(self, app: Any, *, paths: Paths, config: Callable[[], AccountExportConfig], spill_dir_name: Callable[[], str | None] = lambda: None) -> None:
        self._app = app
        self._paths = paths
        self._config = config
        self._spill_dir_name = spill_dir_name
        self._jobs: dict[str, ExportJob] = {}
        self._removals: set[asyncio.Future] = set()
        remove_left_exports(paths)

    def status(self, user_id: str) -> ExportJob | None:
        return self._jobs.get(user_id)

    def start(self, user: Any) -> ExportJob:
        """Start the person's export, or return the one they already have."""
        user_id = str(user.id)
        existing = self._jobs.get(user_id)
        if existing is not None and existing.state in ("building", "ready"):
            return existing
        config = self._config()
        if sum(job.state == "building" for job in self._jobs.values()) >= config.max_concurrent:
            logger.info("Account export for user %s refused: busy", user_id)
            raise ExportBusy("other exports are being prepared; try again in a few minutes")
        if existing is not None:
            self.discard(user_id)
        root = self._paths.exports_dir()
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        directory = root / secrets.token_hex(16)
        directory.mkdir(mode=0o700)
        job = ExportJob(user_id=user_id, directory=directory, started_at=datetime.now(UTC))
        self._jobs[user_id] = job
        job.task = asyncio.get_running_loop().create_task(self._run(job, user, config))
        return job

    def discard(self, user_id: str) -> bool:
        """Stop the person's export if it is building, and delete whatever it wrote; whether there was one."""
        job = self._jobs.pop(user_id, None)
        if job is None:
            return False
        job.cancel.set()
        if job.expiry is not None:
            job.expiry.cancel()
        if job.task is not None and not job.task.done():
            # Its directory goes once the task is over, and with it any worker thread writing to it.
            job.task.cancel()
            job.task.add_done_callback(lambda _: self._remove_later(job.directory))
        else:
            self._remove_later(job.directory)
        logger.info("Account export for user %s removed (%s)", user_id, job.state)
        return True

    def end_for_owners(self, owners: frozenset[str]) -> dict[str, Ended]:
        """A refused person's export is thrown away: nobody may download it now (``OwnerHoldings`` source)."""
        ended = {owner: Ended(1) for owner in owners if self.discard(owner)}
        if ended:
            logger.info("Discarded %d account exports of refused accounts", len(ended))
        return ended

    def open_part(self, user_id: str, number: int) -> PartDownload | None:
        """A part of a prepared export, now being downloaded; ``None`` when there is none. ``close_part`` ends it."""
        job = self._jobs.get(user_id)
        if job is None or job.state not in ("ready", "downloaded") or not 1 <= number <= len(job.parts):
            return None
        job.streams += 1
        if job.expiry is not None:
            job.expiry.cancel()
            job.expiry = None
        job.expires_at = None
        return PartDownload(job=job, number=number, path=job.parts[number - 1], size=job.part_sizes[number - 1])

    def close_part(self, download: PartDownload, *, complete: bool) -> None:
        """A download ended: once every part has gone out in full, the export waits only a little longer.

        A part that went out in full can still be lost on its way: the last
        bytes may sit in a socket buffer when the browser gives up, or the
        person may cancel saving it. So a part is not deleted the moment it
        is sent: it can be downloaded again until the export goes,
        ``REDOWNLOAD_SECONDS`` after its last part was downloaded.
        """
        job = download.job
        job.streams -= 1
        if self._jobs.get(job.user_id) is not job:
            # Discarded, or replaced by a newer export, while it streamed.
            return
        if complete:
            job.sent.add(download.number)
            if job.state == "ready" and len(job.sent) == len(job.parts):
                job.state = "downloaded"
                logger.info("Account export for user %s downloaded in full", job.user_id)
        if job.streams == 0:
            idle = self._config().expires_after_seconds
            self._arm_expiry(job, min(REDOWNLOAD_SECONDS, idle) if job.state == "downloaded" else idle)

    async def close(self) -> None:
        tasks = [job.task for job in self._jobs.values() if job.task is not None and not job.task.done()]
        for user_id in list(self._jobs):
            self.discard(user_id)
        if tasks:
            await asyncio.wait(tasks, timeout=10)
        await self.settled()

    async def settled(self) -> None:
        """Wait for the deletions already started."""
        while self._removals:
            await asyncio.wait(list(self._removals), timeout=10)

    def _remove_later(self, path: Path) -> None:
        removal = asyncio.ensure_future(asyncio.to_thread(_remove, path))
        self._removals.add(removal)
        removal.add_done_callback(self._removals.discard)

    def _arm_expiry(self, job: ExportJob, seconds: float) -> None:
        if job.expiry is not None:
            job.expiry.cancel()
        job.expires_at = datetime.now(UTC) + timedelta(seconds=seconds)
        job.expiry = asyncio.get_running_loop().call_later(seconds, self._expire, job)

    async def _off_loop(self, job: ExportJob, function: Callable[..., Any], *args: Any) -> Any:
        """Blocking work on a worker thread; a cancelled export waits for it to stop (``job.cancel``) before going on."""
        work = asyncio.ensure_future(asyncio.to_thread(function, *args))
        try:
            return await asyncio.shield(work)
        except asyncio.CancelledError:
            job.cancel.set()
            await asyncio.wait([work])
            if not work.cancelled():
                work.exception()
            raise

    async def _run(self, job: ExportJob, user: Any, config: AccountExportConfig) -> None:
        started = time.monotonic()
        try:
            await self._build(job, user, config)
        except ExportRefused as exc:
            self._settle_failure(job, exc.code, exc.detail, config)
            logger.info("Account export for user %s refused while %s: %s", job.user_id, job.stage, exc.code)
            return
        except Exception as exc:  # noqa: BLE001 - the page must hear that it failed; the message may name a file, so only its type is logged
            if _disk_full(exc):
                self._settle_failure(job, NoSpace.code, _no_space().detail, config)
                logger.info("Account export for user %s refused while %s: %s", job.user_id, job.stage, NoSpace.code)
                return
            self._settle_failure(job, "failed", "the export could not be prepared; try again", config)
            logger.warning("Account export for user %s failed while %s: %s", job.user_id, job.stage, type(exc).__name__)
            return
        job.state = "ready"
        self._arm_expiry(job, config.expires_after_seconds)
        progress = job.progress
        logger.info(
            "Account export for user %s ready in %.1f s: %d conversations, %d files, %d bytes, %d parts, %d left out",
            job.user_id,
            time.monotonic() - started,
            progress.conversations_done,
            progress.files_done,
            sum(job.part_sizes),
            len(job.parts),
            job.skipped,
        )

    def _settle_failure(self, job: ExportJob, code: str, detail: str, config: AccountExportConfig) -> None:
        try:
            job.state, job.error, job.detail = "failed", code, detail
            job.parts, job.part_sizes = [], []
            self._remove_later(job.directory)
            # The page reads the failure once; the record goes when an export would have.
            self._arm_expiry(job, config.expires_after_seconds)
        except Exception as exc:  # noqa: BLE001 - never let clean-up surface the export's own error, which may name a file
            logger.warning("Account export for user %s could not be cleaned up: %s", job.user_id, type(exc).__name__)

    def _expire(self, job: ExportJob) -> None:
        if self._jobs.get(job.user_id) is job and job.streams == 0:
            if job.state == "ready":
                logger.info("Account export for user %s expired before it was downloaded in full", job.user_id)
            elif job.state == "downloaded":
                logger.info("Account export for user %s deleted after its download", job.user_id)
            self.discard(job.user_id)

    def _others_to_write(self, job: ExportJob) -> int:
        """What the other exports being prepared still have to write to the same disk."""
        return sum(max(other.progress.bytes_total - other.progress.bytes_done, 0) for other in self._jobs.values() if other is not job and other.state == "building")

    def _fit(self, job: ExportJob, plan: _Plan, config: AccountExportConfig) -> None:
        """Leave out what can never fit, and refuse an export the disk has no room for now."""
        usage = shutil.disk_usage(job.directory)
        # Larger than the whole disk (a sparse file, say): no administrator can make room for it.
        too_large = [found for found in plan.files if found.size > usage.total - config.min_free_bytes]
        if too_large:
            plan.files = [found for found in plan.files if found.size <= usage.total - config.min_free_bytes]
            plan.skipped += [{"path": found.entry, "reason": "too_large"} for found in too_large]
        if sum(found.size for found in plan.files) > usage.free - self._others_to_write(job) - config.min_free_bytes:
            raise _no_space()

    def _check_room(self, job: ExportJob, size: int, config: AccountExportConfig) -> None:
        if size > shutil.disk_usage(job.directory).free - self._others_to_write(job) - config.min_free_bytes:
            raise _no_space()

    @staticmethod
    def _directory_chain(directories: Iterable[Path]) -> tuple[tuple[Path, int, int], ...] | None:
        """Each directory's identity, or ``None`` when one is missing, unreadable, a link or not a directory."""
        chain = []
        for directory in directories:
            try:
                metadata = os.lstat(directory)
            except OSError:
                return None
            if _is_link(directory, metadata) or not stat.S_ISDIR(metadata.st_mode):
                return None
            chain.append((directory, metadata.st_dev, metadata.st_ino))
        return tuple(chain)

    def _plan(self, user_id: str, cancel: threading.Event) -> _Plan:
        """Every file of the person's to archive, found under their own user directory and nowhere else."""
        plan = _Plan()
        seen: set[str] = set()
        spill = self._spill_dir_name()
        reserved = reserved_dir_names({MCP_INTERNAL_DIRNAME, *([spill] if spill else [])})
        user_dir = self._paths.user_dir(user_id)
        home = self._directory_chain([user_dir])
        if home is None:
            return plan
        threads = user_dir / "threads"
        if self._directory_chain([threads]) is not None:
            try:
                with os.scandir(threads) as scan:
                    listed = sorted((entry.name, entry.is_symlink() or entry.is_junction()) for entry in scan if entry.is_dir(follow_symlinks=True))
            except OSError:
                listed = []
                plan.skipped.append({"path": "conversations", "reason": "unreadable"})
            for thread_id, linked in listed:
                if _unsafe_segment(thread_id):
                    plan.skipped.append({"path": f"conversations/{thread_id}", "reason": "unsafe_name"})
                    continue
                if linked:
                    plan.skipped.append({"path": f"conversations/{thread_id}", "reason": "link"})
                    continue
                user_data = threads / thread_id / "user-data"
                chain = self._directory_chain([user_dir, threads, threads / thread_id, user_data])
                if chain is None:
                    if user_data.is_symlink() or user_data.is_junction():
                        plan.skipped.append({"path": f"conversations/{thread_id}/files", "reason": "link"})
                    continue
                plan.thread_folders.append(thread_id)
                for area in USER_DATA_AREAS:
                    _walk(threads / thread_id / "user-data" / area, f"conversations/{thread_id}/files/{area}", chain, plan, seen, reserved, cancel)
        _walk(self._paths.user_files_dir(user_id), "my-files", home, plan, seen, reserved, cancel)
        skills = self._paths.user_custom_skills_dir(user_id)
        skills_chain = self._directory_chain([user_dir, *reversed([parent for parent in skills.parents if parent.is_relative_to(user_dir) and parent != user_dir])])
        if skills_chain is not None:
            _walk(skills, "my-skills", skills_chain, plan, seen, reserved, cancel)
        return plan

    async def _conversations(self, user_id: str) -> list[dict[str, Any]]:
        """Every thread the store records under this person's own id, and no other."""
        records: dict[str, dict[str, Any]] = {}
        for record in await self._app.state.thread_store.search(limit=_ALL_THREADS, offset=0, user_id=user_id):
            records.setdefault(str(record["thread_id"]), record)
        return list(records.values())

    async def _schedules(self, user_id: str) -> list[dict[str, Any]] | None:
        repo = getattr(self._app.state, "scheduled_task_repo", None)
        if repo is None:
            return None
        return [{key: row.get(key) for key in SCHEDULE_FIELDS if key in row} for row in await repo.list_by_user(user_id)]

    @staticmethod
    async def _memory(user_id: str, agent_name: str | None, skipped: list[dict[str, str]]) -> dict[str, Any] | None:
        """A memory document, or ``None``; one that cannot be read is named, and the rest still exports."""
        try:
            return await memory_export_document(user_id, agent_name=agent_name)
        except Exception:  # noqa: BLE001 - a corrupt memory must not keep the person's other work from them
            skipped.append({"path": f"agents/{agent_name}/memory.json" if agent_name else "memory.json", "reason": "unreadable"})
            return None

    async def _conversation(self, job: ExportJob, parts: _Parts, record: dict[str, Any], exported_at: datetime, written: list[dict[str, Any]], skipped: list[dict[str, str]]) -> dict[str, Any] | None:
        """Write one conversation's transcripts; its manifest entry, or ``None`` when its id cannot name a folder."""
        thread_id = str(record["thread_id"])
        if _unsafe_segment(thread_id):
            skipped.append({"path": f"conversations/{thread_id}", "reason": "unsafe_name"})
            return None
        entry = {
            "id": thread_id,
            "title": str(record.get("display_name") or "Untitled"),
            "created_at": record.get("created_at") or None,
            "updated_at": record.get("updated_at") or None,
            "message_count": None,
            "transcripts": [],
        }
        try:
            conversation = await transcript.read_conversation(SimpleNamespace(app=self._app), thread_id, record, user_id=job.user_id)
        except Exception:  # noqa: BLE001 - one conversation that cannot be read must not keep the rest from the person
            skipped.append({"path": f"conversations/{thread_id}/transcript", "reason": "unreadable"})
            return entry
        if conversation is not None:
            entry["title"], entry["created_at"] = conversation.title, conversation.created_at
        messages = conversation.messages if conversation is not None else []
        markdown = transcript.transcript_markdown(title=entry["title"], created_at=entry["created_at"], messages=messages, exported_at=exported_at)
        document = transcript.transcript_json(title=entry["title"], thread_id=thread_id, created_at=entry["created_at"], messages=messages, exported_at=exported_at)
        entry["transcripts"] = [f"conversations/{thread_id}/transcript.md", f"conversations/{thread_id}/transcript.json"]
        written.append(await self._off_loop(job, parts.add_bytes, entry["transcripts"][0], markdown.encode("utf-8")))
        written.append(await self._off_loop(job, parts.add_bytes, entry["transcripts"][1], document.encode("utf-8")))
        entry["message_count"] = len(json.loads(document)["messages"])
        return entry

    async def _build(self, job: ExportJob, user: Any, config: AccountExportConfig) -> None:
        user_id = job.user_id
        progress = job.progress
        records = await self._conversations(user_id)
        job.stage = "finding files"
        plan = await self._off_loop(job, self._plan, user_id, job.cancel)
        self._fit(job, plan, config)
        progress.conversations_total = len(records)
        progress.files_total = len(plan.files)
        progress.bytes_total = sum(found.size for found in plan.files)
        logger.info("Account export for user %s started: %d conversations, %d files, %d bytes", user_id, progress.conversations_total, progress.files_total, progress.bytes_total)

        exported_at = datetime.now(UTC)
        parts = _Parts(job.directory, config.part_bytes)
        written: list[dict[str, Any]] = []
        conversations: list[dict[str, Any]] = []
        skipped = plan.skipped
        try:
            job.stage = "writing transcripts"
            for record in records:
                entry = await self._conversation(job, parts, record, exported_at, written, skipped)
                if entry is not None:
                    conversations.append(entry)
                progress.conversations_done += 1

            job.stage = "copying files"
            own_files = own_bytes = 0
            # Each file's plan entry goes as it is copied: a large account holds many.
            plan.files.reverse()
            while plan.files:
                found = plan.files.pop()
                if job.cancel.is_set():
                    raise asyncio.CancelledError
                self._check_room(job, found.size, config)
                try:
                    copied = await self._off_loop(job, parts.add_file, found, job.cancel)
                except ArtifactArchiveError as exc:
                    reason = _left_out(exc)
                    if reason is None:
                        raise
                    skipped.append({"path": found.entry, "reason": reason})
                else:
                    written.append(copied)
                    own_files += 1
                    own_bytes += copied["size"]
                progress.files_done += 1
                progress.bytes_done += found.size

            job.stage = "writing definitions"
            memory = await self._memory(user_id, None, skipped)
            if memory is not None:
                written.append(await self._off_loop(job, parts.add_bytes, "memory.json", _json(memory)))
            schedules = await self._schedules(user_id)
            if schedules is not None:
                written.append(await self._off_loop(job, parts.add_bytes, "scheduled-tasks.json", _json(schedules)))
            agents = await self._off_loop(job, owned_agent_documents, user_id)
            if agents is not None:
                written.append(await self._off_loop(job, parts.add_bytes, "agents.json", _json(agents)))
                for agent in agents:
                    name = str(agent.get("name") or "")
                    if not name or _unsafe_segment(name):
                        continue
                    remembered = await self._memory(user_id, name, skipped)
                    if remembered is not None:
                        written.append(await self._off_loop(job, parts.add_bytes, f"agents/{name}/memory.json", _json(remembered)))

            job.stage = "writing the manifest"
            listed = {conversation["id"] for conversation in conversations}
            manifest: dict[str, Any] = {
                "format": FORMAT,
                "version": FORMAT_VERSION,
                "exported_at": exported_at.isoformat(),
                "release": release(),
                "person": {"id": user_id, "email": str(getattr(user, "email", "") or "")},
                "conversations": conversations,
                "folders_without_conversation": [folder for folder in plan.thread_folders if folder not in listed],
                "files": written,
                "skipped": skipped,
                "parts": None,
                "totals": None,
            }
            readme = _readme(manifest, own_files=own_files, own_bytes=own_bytes, has={"memory": memory is not None, "schedules": schedules is not None, "agents": agents is not None})
            written.append(await self._off_loop(job, parts.add_bytes, README_NAME, readme))
            manifest["totals"] = {"conversations": len(conversations), "files": len(written), "bytes": sum(item["size"] for item in written), "skipped": len(skipped)}
            # Last, in the last part, so it can say how many there are.
            # Compact: for a program, and one line per file of a large account.
            await self._off_loop(job, parts.add_last, MANIFEST_NAME, lambda count: json.dumps({**manifest, "parts": count}, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8"))
            await self._off_loop(job, parts.close)
        except BaseException:
            parts.abandon()
            raise
        job.parts = list(parts.paths)
        job.part_sizes = [path.stat().st_size for path in job.parts]
        job.skipped = len(skipped)
