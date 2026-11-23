"""Non-executable parsed tables owned by an ordinary chat workspace."""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import json
import locale
import math
import os
import re
import stat
import sys
from contextlib import ExitStack, contextmanager
from functools import lru_cache
from pathlib import Path

import pandas as pd

WORKSPACE = Path("/mnt/user-data/workspace")
HOME = Path("/mnt/spaces/home")
MAX_BYTES = 8 * 1024 * 1024
MAX_ENTRIES = 10  # one full ten-export report fits between builds
MAX_COLUMNS = 256
MAX_CELLS = 200_000
DTYPE = re.compile(r"object|str|string|bool|int64|float64|datetime64\[(?:s|ms|us|ns)\]")
CACHE_ERRORS = (OSError, ValueError, TypeError, KeyError, OverflowError, RecursionError, NotImplementedError, ImportError, AttributeError)


@lru_cache(maxsize=1)
def parser_identity():
    # Header recovery and sheet selection depend on the skill's role inference.
    # A parser edit invalidates old entries without a manual version bump.
    import dateutil
    import numpy as np
    import openpyxl

    try:
        import xlrd
    except ImportError:
        xlrd = None
    digest = hashlib.sha256()
    for name in ("report.py", "business_report_common.py", "business_report_input_cache.py"):
        digest.update(hashlib.sha256(Path(__file__).with_name(name).read_bytes()).digest())
    return [digest.hexdigest(), sys.version, pd.__version__, np.__version__, openpyxl.__version__, getattr(xlrd, "__version__", None), dateutil.__version__]


def entry(digest, suffix, sheet, profile):
    if os.name != "posix" or not all(hasattr(os, name) for name in ("O_DIRECTORY", "O_NOFOLLOW", "O_NONBLOCK", "geteuid")):
        return None
    try:
        import fcntl

        if not hasattr(fcntl, "flock"):
            return None
        key = [parser_identity(), locale.setlocale(locale.LC_TIME), digest, suffix, sheet, profile]
        return hashlib.sha256(json.dumps(key, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    except CACHE_ERRORS:
        return None


@contextmanager
def _directory():
    with ExitStack() as stack:
        def directory(name, parent=None):
            fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
            stack.callback(os.close, fd)
            return fd

        root = directory("/")
        for part in WORKSPACE.parts[1:]:
            root = directory(part, root)
        # The instance runtime maps workspace to employee Home. A bind mount
        # has the same inode even when its path contains no symbolic link.
        try:
            shared = HOME.stat()
        except FileNotFoundError:
            shared = None
        if shared and os.path.samestat(shared, os.fstat(root)):
            raise ValueError("shared Home")
        for name in (".cache", "business-report-inputs-v1"):
            try:
                os.mkdir(name, mode=0o700, dir_fd=root)
            except FileExistsError:
                pass
            root = directory(name, root)
        info = os.fstat(root)
        if info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise ValueError("not a private cache")
        yield root


def _secret(root, *, create=False):
    # Bind records to the owning chat even if two chats use identical inputs.
    # As with the source, the sandbox account itself is trusted.
    try:
        fd = os.open("key", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=root)
    except FileNotFoundError:
        if not create:
            raise
        key = os.urandom(32)
        fd = os.open("key", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=root)
        with os.fdopen(fd, "wb") as stream:
            stream.write(key)
        return key
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_mode & 0o077 or info.st_uid != os.geteuid() or info.st_size != 32:
            raise ValueError("private cache key")
        key = stream.read(33)
        if len(key) != 32:
            raise ValueError("private cache key")
        return key


def _signature(secret, key, payload):
    mac = hmac.new(secret, key.encode("ascii"), hashlib.sha256)
    mac.update(payload)
    return mac.hexdigest().encode("ascii") + b"\n"


def _cell(value):
    if pd.isna(value):
        return None
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return [type(value).__name__, value.isoformat()]
    if type(value) in (str, int, bool, float):
        return value
    raise ValueError("unsupported cell type")


def _decode(value):
    if value is None or type(value) in (str, int, bool) or (type(value) is float and math.isfinite(value)):
        return value
    if not isinstance(value, list) or len(value) != 2 or not all(isinstance(item, str) for item in value):
        raise ValueError("unsupported cell")
    kind, text = value
    return {"Timestamp": pd.Timestamp, "datetime": dt.datetime.fromisoformat, "date": dt.date.fromisoformat, "time": dt.time.fromisoformat}[kind](text)


def _restore(payload):
    sheet, skipped, workbook, columns = json.loads(payload)
    if type(workbook) is not bool or (workbook and not isinstance(sheet, str)) or (not workbook and sheet is not None):
        raise ValueError("sheet")
    if not isinstance(skipped, list) or any(not isinstance(item, dict) or set(item) != {"sheet", "reason"} or not all(isinstance(v, str) for v in item.values()) for item in skipped):
        raise ValueError("skipped sheets")
    if not isinstance(columns, list) or not 0 < len(columns) <= MAX_COLUMNS:
        raise ValueError("columns")
    values_by_name = {}
    rows = None
    for label, dtype, values in columns:
        if not isinstance(label, str) or label in values_by_name or not DTYPE.fullmatch(dtype) or not isinstance(values, list):
            raise ValueError("column")
        if rows is None:
            rows = len(values)
        if len(values) != rows or rows * len(columns) > MAX_CELLS:
            raise ValueError("row count")
        values_by_name[label] = pd.Series([_decode(value) for value in values], dtype=dtype)
    return sheet, pd.DataFrame(values_by_name), skipped, workbook


def _slot(root, index):
    name = f"{index}.json"
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=root)
    return os.fdopen(fd, "rb")


def load(key):
    if key is None:
        return None
    try:
        with _directory() as root:
            secret = _secret(root)
            for index in range(MAX_ENTRIES):
                try:
                    with _slot(root, index) as stream:
                        info = os.fstat(stream.fileno())
                        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > MAX_BYTES or stream.readline(66) != key.encode("ascii") + b"\n":
                            continue
                        checksum = stream.readline(66)
                        payload = stream.read(MAX_BYTES + 1)
                        if len(payload) <= MAX_BYTES and hmac.compare_digest(checksum, _signature(secret, key, payload)):
                            return _restore(payload)
                except FileNotFoundError:
                    continue
    except CACHE_ERRORS:
        pass
    return None


def _encode(sheet, frame, skipped, workbook):
    if frame.memory_usage(index=False, deep=True).sum() > MAX_BYTES or not frame.columns.is_unique:
        return None
    if len(skipped) > 256 or any(len(v) > 32767 for item in skipped for v in item.values()):
        return None
    columns = []
    for label, column in frame.items():
        dtype = str(column.dtype)
        if not isinstance(label, str) or len(label) > 32767 or not DTYPE.fullmatch(dtype):
            return None
        columns.append([label, dtype, [_cell(value) for value in column.tolist()]])
    payload = json.dumps([sheet, skipped, workbook, columns], ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
    # Two SHA-256 hex lines precede the body.
    return payload if 130 + len(payload) <= MAX_BYTES else None


def _destination(root, key):
    ages = []
    for index in range(MAX_ENTRIES):
        try:
            with _slot(root, index) as stream:
                info = os.fstat(stream.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise ValueError("cache slot")
                if stream.readline(66) == key.encode("ascii") + b"\n":
                    return index
                ages.append((info.st_mtime_ns, index))
        except FileNotFoundError:
            return index
    return min(ages)[1]


def _clear_pending(root):
    try:
        os.unlink("pending", dir_fd=root)
    except FileNotFoundError:
        pass


def store(key, sheet, frame, skipped, workbook):
    if key is None or len(frame.columns) > MAX_COLUMNS or frame.size > MAX_CELLS:
        return
    try:
        import fcntl

        with _directory() as root:
            payload = _encode(sheet, frame, skipped, workbook)
            if payload is None:
                return
            # One private temporary and a fixed set of slots bound both
            # concurrent writers and interrupted writes. A busy cache is optional.
            fcntl.flock(root, fcntl.LOCK_EX | fcntl.LOCK_NB)
            index = _destination(root, key)
            header = key.encode("ascii") + b"\n" + _signature(_secret(root, create=True), key, payload)
            _clear_pending(root)
            fd = os.open("pending", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=root)
            try:
                with os.fdopen(fd, "wb") as stream:
                    stream.write(header)
                    stream.write(payload)
                os.replace("pending", f"{index}.json", src_dir_fd=root, dst_dir_fd=root)
            finally:
                _clear_pending(root)
    except CACHE_ERRORS:
        pass
