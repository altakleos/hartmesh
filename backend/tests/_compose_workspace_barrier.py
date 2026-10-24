"""Read-only preconditions for a deliberately removed qualification workspace.

The caller supplies bounded reads. This helper neither grants native execution
authority nor proves that every native resource has finished finalization.
"""

import math
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime


class WorkspaceBarrierError(AssertionError):
    """A captured removal has not been safely observed."""


def _refuse():
    raise WorkspaceBarrierError("Workspace retirement observation is invalid") from None


def _text(value):
    return type(value) is str and 0 < len(value) <= 128 and all(character.isprintable() and not character.isspace() for character in value)


@dataclass(frozen=True)
class CapturedWorkspace:
    instance_id: str
    owner_id: str
    instance_generation: int
    home_id: str
    home_generation: int
    attachment_id: str
    incarnation: str
    host_id: str
    container_id: str
    container_name: str
    relay_id: str
    relay_name: str

    def __post_init__(self):
        for value in (self.instance_id, self.home_id, self.attachment_id, self.incarnation):
            if type(value) is not str or re.fullmatch(r"[0-9a-f]{32}", value) is None:
                _refuse()
        for value in (self.container_id, self.relay_id):
            if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None:
                _refuse()
        for value in (self.container_name, self.relay_name):
            # Retain actual Docker Name form; strip one slash only at lookup.
            if type(value) is not str or re.fullmatch(r"/[A-Za-z0-9][A-Za-z0-9_.-]{0,254}", value) is None:
                _refuse()
        if len({self.container_id, self.container_name[1:], self.relay_id, self.relay_name[1:]}) != 4:
            _refuse()
        if not _text(self.owner_id) or not _text(self.host_id):
            _refuse()
        for value in (self.instance_generation, self.home_generation):
            if type(value) is not int or value <= 0:
                _refuse()


def _scope(captured, row):
    if type(captured) is not CapturedWorkspace or not isinstance(row, Mapping):
        _refuse()
    expected = dict(
        attachment_id=captured.attachment_id,
        incarnation=captured.incarnation,
        actor_kind="nonhuman",
        actor_id="agent:" + captured.instance_id,
        host_id=captured.host_id,
        container_id=captured.container_id,
        instance_id=captured.instance_id,
        instance_owner_id=captured.owner_id,
        instance_generation=captured.instance_generation,
        instance_status="active",
        instance_custody="personal",
        home_id=captured.home_id,
        home_generation=captured.home_generation,
        home_status="active",
        home_custody_kind="personal",
        home_custodian_kind="human",
        home_custodian_id=captured.owner_id,
        mount_space_id=captured.home_id,
        mount_generation=captured.home_generation,
        mount_alias="home",
        mount_writable=1,
    )
    for key, value in expected.items():
        if key not in row or type(row[key]) is not type(value) or row[key] != value:
            _refuse()
    if not all(key in row for key in ("phase", "fenced_by_kind", "fenced_by_id", "fence_reason", "fenced_at")):
        _refuse()


def _phase(captured, row):
    _scope(captured, row)
    phase = row["phase"]
    if type(phase) is not str or phase not in ("active", "fence_pending", "fenced"):
        _refuse()
    closure = (row["fenced_by_kind"], row["fenced_by_id"], row["fence_reason"], row["fenced_at"])
    if phase == "active":
        if closure != (None, None, None, None):
            _refuse()
    else:
        if any(type(value) is not str for value in closure[:3]) or closure[:3] != ("host", captured.host_id, "container-absent"):
            _refuse()
        if phase == "fence_pending":
            if closure[3] is not None:
                _refuse()
        else:
            if type(closure[3]) is not str:
                _refuse()
            try:
                timestamp = datetime.fromisoformat(closure[3])
            except ValueError:
                _refuse()
            if timestamp.tzinfo is None or timestamp.utcoffset() is None:
                _refuse()
    return phase


def validate_workspace_capture(captured: CapturedWorkspace, row: Mapping[str, object] | None) -> None:
    """Validate complete initial active scope without inferring human custody."""
    if _phase(captured, row) != "active":
        _refuse()


def wait_for_workspace_retirement(
    captured: CapturedWorkspace,
    *,
    read_attachment: Callable[[float], Mapping[str, object] | None],
    is_absent: Callable[[str, float], bool],
    read_execution: Callable[[float], Mapping[str, object]],
    timeout: float = 90,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Observe exact old scope, four absences, then fresh execution readiness.

    All reads and polling consume the original deadline. A callback must enforce
    its supplied timeout; results that complete at the deadline are rejected.
    """
    if type(captured) is not CapturedWorkspace or type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 90:
        _refuse()
    previous_time = None

    def now():
        nonlocal previous_time
        try:
            value = clock()
        except Exception:
            _refuse()
        if type(value) not in (int, float) or not math.isfinite(value) or (previous_time is not None and value < previous_time):
            _refuse()
        previous_time = value
        return value

    deadline = now() + timeout

    def remaining():
        value = deadline - now()
        if value <= 0:
            raise WorkspaceBarrierError("Workspace retirement exceeded its deadline") from None
        return value

    def read(callback, *args):
        budget = min(10, remaining())
        try:
            result = callback(*args, budget)
        except Exception:
            _refuse()
        remaining()
        return result

    previous_phase = -1
    references = (captured.container_id, captured.container_name[1:], captured.relay_id, captured.relay_name[1:])
    while True:
        row = read(read_attachment)
        phase = _phase(captured, row)
        rank = ("active", "fence_pending", "fenced").index(phase)
        if rank < previous_phase:
            _refuse()
        previous_phase = rank
        if phase == "fenced":
            absent = True
            for reference in references:
                result = read(is_absent, reference)
                if type(result) is not bool:
                    _refuse()
                if result is False:
                    absent = False
                    break
            if absent:
                execution = read(read_execution)
                if not isinstance(execution, Mapping) or not all(key in execution for key in ("enabled", "execution", "reason")):
                    _refuse()
                if type(execution["enabled"]) is not bool or type(execution["execution"]) is not str or not execution["execution"] or (execution["reason"] is not None and type(execution["reason"]) is not str):
                    _refuse()
                if execution["enabled"] is True and execution["execution"] == "ready" and execution["reason"] is None:
                    remaining()
                    return
        interval = min(1, remaining())
        try:
            sleep(interval)
        except Exception:
            _refuse()
        remaining()
