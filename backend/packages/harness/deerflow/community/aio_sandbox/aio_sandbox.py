import base64
import contextlib
import errno
import logging
import math
import shlex
import threading
import time
import uuid
from dataclasses import dataclass, field

import httpx
from agent_sandbox import Sandbox as AioSandboxClient
from agent_sandbox.core.api_error import ApiError

from deerflow.config.paths import VIRTUAL_PATH_PREFIX
from deerflow.sandbox.remote_list_dir import (
    parse_remote_list_dir_output,
    remote_list_dir_command,
)
from deerflow.sandbox.sandbox import (
    ABORT_TOKEN_ENV,
    Sandbox,
    _validate_extra_env,
    current_sandbox_command_call,
)
from deerflow.sandbox.search import (
    GrepMatch,
    path_matches,
    should_ignore_path,
    truncate_line,
)

from .backend import sandbox_http_trust_env

logger = logging.getLogger(__name__)

_MAX_DOWNLOAD_SIZE = 100 * 1024 * 1024  # 100 MB

_ERROR_OBSERVATION_SIGNATURE = "'ErrorObservation' object has no attribute 'exit_code'"

# How many aborted calls a sandbox remembers. The memory only has to outlive
# the command each abort was aimed at; nothing here can see a tool call end, so
# it is bounded rather than cleared.
_ABORTED_CALL_MEMORY = 512

# How many shell sessions' abort tokens a sandbox remembers.
_SESSION_TOKEN_MEMORY = 256

# How much older than its command a process may look and still be taken for
# that command's own. The command's age is measured on this side and each
# process's age in the container, in whole seconds, so the allowance covers the
# rounding on both and the time the sweep request takes to arrive.
_ABORT_SWEEP_AGE_ALLOWANCE_SECONDS = 3

# The abort's kill sweep, run in the container on a fresh shell session while
# the command it is ending still holds the client's serialization lock. It
# finds the command's processes by the token in their environment, because a
# child that detached itself (``setsid``, ``nohup``) has left the shell's
# process group but not the environment it inherited. Written for POSIX ``sh``
# with busybox-compatible ``grep`` flags so it does not depend on the image
# carrying bash or GNU coreutils. It ends with ``true`` rather than ``exit``:
# the sweep is one command in a persistent session, and closing that session's
# shell would leave the request waiting for an answer that never comes.
_ABORT_SWEEP_HEAD = "for d in /proc/[0-9]*; do p=${d#/proc/}; if tr '\\0' '\\n' 2>/dev/null < \"$d/environ\" | grep -qxF "
_ABORT_SWEEP_KILL = 'kill -9 "$p" 2>/dev/null'
# A persistent shell's token marks everything that shell ever started, so the
# sweep also asks how old each process is and spares the ones that were already
# running before the command began (a server an earlier command left in the
# background). Field 22 of ``/proc/<pid>/stat`` is the start time in clock
# ticks; the text up to the last ``)`` is cut first because a process name may
# itself contain spaces. "Now" is the start time of a process the sweep has
# just started, read the same way, so both ends of the subtraction come from
# one clock -- ``/proc/uptime`` does not, where a container runtime rewrites
# it. A process whose age cannot be read is killed: the caller asked for the
# command to stop.
_ABORT_SWEEP_NOW = "t=$(getconf CLK_TCK 2>/dev/null); n=$(cat /proc/self/stat 2>/dev/null); n=${n##*) }; set -- $n; n=${20:-}; "
_ABORT_SWEEP_AGE_TEST = 's=$(cat "$d/stat" 2>/dev/null); s=${s##*) }; set -- $s; if [ -z "${20:-}" ] || [ -z "$n" ] || [ $(( (n - ${20}) / ${t:-100} )) -le %d ]; then '


def _abort_sweep_command(token: str, *, started_within_seconds: int | None = None, protected_processes: frozenset[tuple[int, int]] | None = None) -> str:
    """The shell text that kills every process carrying ``token``.

    ``started_within_seconds`` limits the kill to processes no older than
    that; ``None`` kills every carrier, which is right when the token belongs
    to one command alone.
    """
    marker = shlex.quote(f"{ABORT_TOKEN_ENV}={token}")
    if protected_processes is not None:
        guard = ""
        test = ""
        if protected_processes:
            identities = "|".join(f"{pid}:{ticks}" for pid, ticks in sorted(protected_processes))
            guard = (
                'df_keep() { q=$1; depth=0; while [ "$q" -gt 1 ] && [ "$depth" -lt 256 ]; do '
                'read -r s < "/proc/$q/stat" 2>/dev/null || return 1; s=${s##*) }; set -- $s; '
                f'case "$q:${{20:-}}" in {identities}) return 0;; esac; '
                "q=${2:-0}; depth=$((depth + 1)); done; return 1; }; "
            )
            test = 'if df_keep "$p"; then continue; fi; '
        return f"{guard}{_ABORT_SWEEP_HEAD}{marker}; then {test}{_ABORT_SWEEP_KILL}; fi; done; true"
    if started_within_seconds is None:
        return f"{_ABORT_SWEEP_HEAD}{marker}; then {_ABORT_SWEEP_KILL}; fi; done; true"
    return f"{_ABORT_SWEEP_NOW}{_ABORT_SWEEP_HEAD}{marker}; then {_ABORT_SWEEP_AGE_TEST % started_within_seconds}{_ABORT_SWEEP_KILL}; fi; fi; done; true"


def _shell_identity_path(token: str) -> str:
    return f"/tmp/.deerflow-shell-{token}.pid"


def _shell_marking_prefix(token: str) -> str:
    """Record the parent shell before running the first user command.

    The subshell keeps positional parameters and umask out of the persistent
    shell's state. PID plus Linux start ticks prevent killing a reused PID.
    """
    path = shlex.quote(_shell_identity_path(token))
    return f'export {ABORT_TOKEN_ENV}={shlex.quote(token)}; (umask 077; read -r s < /proc/$$/stat; s=${{s##*) }}; set -- $s; printf "%s %s\\n" "$$" "${{20}}" > {path}); '


def _abort_shell_command(token: str) -> str:
    """Kill the shell before any child, so it cannot run trailing statements."""
    path = shlex.quote(_shell_identity_path(token))
    return (
        f"if read -r p expected < {path} 2>/dev/null; then "
        'case "$p:$expected" in *[!0-9:]*|:*|*:) ;; *) '
        'if read -r s < "/proc/$p/stat" 2>/dev/null; then '
        's=${s##*) }; set -- $s; if [ "${20:-}" = "$expected" ]; then kill -9 "$p" 2>/dev/null; fi; fi;; esac; fi; '
        f"rm -f {path}; true"
    )


# Env-bearing commands require the bash.exec API (POST /v1/bash/exec), which the
# all-in-one-sandbox image only ships since 1.9.x. Older images (including any
# ``latest`` tag frozen on the 1.0.0.x line) answer 404 for the whole /v1/bash/*
# namespace. That raw 404 is useless to the model (it just retries), so the
# sandbox fails fast with this operator-facing message instead (#3921).
_BASH_EXEC_UNSUPPORTED_ERROR = (
    "Error: this sandbox image does not support per-command environment injection "
    "(POST /v1/bash/exec returned 404), which is required to run skills that declare "
    "required-secrets. This is a deployment issue that retrying cannot fix: upgrade the "
    "sandbox image to all-in-one-sandbox >= 1.9.3 (set `sandbox.image` in config.yaml, "
    "e.g. pin the tag `1.11.0`) and recreate the sandbox container, then try again."
)


@dataclass
class _InflightCommand:
    """One command executing in the container right now."""

    #: The tool call it belongs to, or None outside one.
    call_id: str | None
    #: The explicit shell session, or None for the env-bearing bash.exec path.
    session_id: str | None
    #: Carried in the environment of every process the command starts.
    token: str
    #: When the command was sent (monotonic clock), for a token its whole shell
    #: session shares; None when the token is this command's alone.
    started: float | None
    protected_processes: frozenset[tuple[int, int]] | None = None
    aborted: bool = False
    abort_complete: threading.Event = field(default_factory=threading.Event)
    abort_wait_timeout: float = 0


@dataclass
class _ScopedShellSession:
    """One server-side shell session serialized within an agent execution."""

    lock: threading.Lock = field(default_factory=threading.Lock)
    session_id: str | None = None


@dataclass
class _SessionCreationState:
    """Process-local ownership state for one server-side session plane."""

    pending: set[str] = field(default_factory=set)
    ambiguous: set[str] = field(default_factory=set)


class AioSandbox(Sandbox):
    """Sandbox implementation using the agent-infra/sandbox Docker container.

    This sandbox connects to a running AIO sandbox container via HTTP API.
    Lead/direct calls retain the legacy serialized shell. Delegated executions
    receive separate server-side sessions, with commands serialized only inside
    the same execution scope, so parallel subagents cannot corrupt one another's
    shell state (see #1433 and #5128).
    """

    #: The legacy exec path reuses one persistent shell session across calls,
    #: so shell state (exports, cwd, functions) carries from one command into
    #: the next — recorded bash evidence cannot prove a clean environment.
    persistent_shell_sessions = True

    def __init__(
        self,
        id: str,
        base_url: str,
        home_dir: str | None = None,
        request_headers: dict[str, str] | None = None,
        default_command_timeout: float | None = None,
    ):
        """Initialize the AIO sandbox.

        Args:
            id: Unique identifier for this sandbox instance.
            base_url: URL of the sandbox API (e.g., http://localhost:8080).
            home_dir: Home directory inside the sandbox. If None, will be fetched from the sandbox.
            request_headers: Trusted control-plane headers required by a local
                relay. These are never injected into sandbox commands.
            default_command_timeout: Provider-configured command deadline used
                when a command does not provide an explicit timeout.
        """
        super().__init__(id)
        if default_command_timeout is None:
            self._default_command_timeout = self._DEFAULT_HARD_TIMEOUT
        else:
            try:
                resolved_default_timeout = float(default_command_timeout)
            except (TypeError, ValueError) as exc:
                raise ValueError("default_command_timeout must be positive") from exc
            if not math.isfinite(resolved_default_timeout) or resolved_default_timeout <= 0:
                raise ValueError("default_command_timeout must be positive")
            self._default_command_timeout = resolved_default_timeout
        self._base_url = base_url
        client_kwargs = {
            "base_url": base_url,
            "timeout": 600,
        }
        if request_headers:
            client_kwargs["headers"] = dict(request_headers)
        if sandbox_http_trust_env(base_url):
            self._client = AioSandboxClient(**client_kwargs)
        else:
            direct_client = httpx.Client(timeout=600, follow_redirects=True, trust_env=False)
            self._client = AioSandboxClient(**client_kwargs, httpx_client=direct_client)
        self._home_dir = home_dir
        self._lock = threading.Lock()
        self._scope_registry_lock = threading.Lock()
        self._scoped_shell_sessions: dict[str, _ScopedShellSession] = {}
        self._recovery_session_id: str | None = None
        self._default_shell_corrupted = False
        self._closed = False
        # Set to True after bash.exec answers 404 (image predates /v1/bash/*),
        # so later env-bearing calls fail fast instead of re-hitting HTTP (#3921).
        self._bash_exec_unsupported = False
        self._session_creation_state_lock = threading.Lock()
        self._shell_session_creation_state = _SessionCreationState()
        self._bash_session_creation_state = _SessionCreationState()
        # Abort state, guarded by its own lock and never ``_lock``: the abort
        # runs while the command it is ending holds that one, so anything the
        # abort touches has to be reachable without it.
        self._abort_lock = threading.Lock()
        self._inflight_commands: dict[int, _InflightCommand] = {}
        self._next_command_seq = 0
        # The token each persistent shell session exports, keyed by session id
        # A session is marked once, on
        # the first command this object sends it, so that every later command
        # reaches the shell exactly as written and the shell's own state --
        # ``$?``, ``PIPESTATUS``, traps -- carries from one call to the next.
        self._session_abort_tokens: dict[str, str] = {}
        # Calls already aborted, newest last. A command belonging to one of
        # them refuses to rotate-and-retry, so a killed command is never
        # quietly re-run, and one that has not spawned yet knows not to.
        # Bounded because nothing here can observe a tool call ending; the
        # oldest entry is dropped once the cap is reached.
        self._aborted_calls: dict[str, None] = {}

    @property
    def base_url(self) -> str:
        return self._base_url

    def detach_default_shell(self) -> tuple[str, str | None] | None:
        """Transfer a drained default shell to the provider's warm-pool entry.

        Detaching prevents close() from destroying its shell and older jobs;
        the host HTTP client is still closed normally. Scoped shells are never
        transferred. Call only after execution leases have drained.
        """
        with self._lock:
            session_id = self._recovery_session_id
            if session_id is None:
                return None
            self._recovery_session_id = None
            with self._abort_state_lock():
                return session_id, self._session_abort_tokens.get(session_id)

    def restore_default_shell(self, state: tuple[str, str | None] | None) -> None:
        """Reconnect a new warm-pool client to the previous default shell."""
        if state is None:
            return
        session_id, token = state
        with self._lock:
            self._recovery_session_id = session_id
            if token is not None:
                with self._abort_state_lock():
                    self._remember_session_token(session_id, token)

    def close(self) -> None:
        """Best-effort close of the host-side HTTP client owned by this sandbox.

        The agent_sandbox SDK is Fern-generated and exposes no ``close()`` /
        ``__exit__``, so we reach the socket-owning ``httpx.Client`` explicitly
        through its attribute chain::

            Sandbox._client_wrapper        -> SyncClientWrapper
                .httpx_client              -> Fern HttpClient (a wrapper, NOT httpx.Client)
                    .httpx_client          -> httpx.Client     <- the real socket owner

        Closing it releases pooled sockets so long-running provider lifecycles
        do not accumulate unreclaimed host-side resources (#2872).

        Resolution is most-specific-first with graceful degradation: if a future
        SDK adds a top-level ``Sandbox.close()`` it is picked up automatically
        without changing this code. Idempotent, thread-safe, and non-fatal:
        failures during teardown are logged and swallowed so provider/backend
        cleanup is never blocked.
        """
        # Close admission for scoped commands before draining their sessions.
        # A scoped call that already registered itself remains in this snapshot
        # and is joined through its per-scope lock below; later calls fail
        # without creating an orphaned registry entry.
        with self._scope_registry_lock:
            if self._closed:
                return
            self._closed = True
            scoped_sessions = list(self._scoped_shell_sessions.items())
            self._scoped_shell_sessions.clear()
        for scope_id, scoped in scoped_sessions:
            with scoped.lock:
                if scoped.session_id is not None and self._client is not None:
                    self._cleanup_session_best_effort(
                        self._client,
                        scoped.session_id,
                        context=f"execution scope {scope_id}",
                        request_options=self._bounded_cleanup_request_options(),
                    )
                    scoped.session_id = None

        with self._lock:
            if self._recovery_session_id is not None and self._client is not None:
                self._cleanup_session_best_effort(
                    self._client,
                    self._recovery_session_id,
                    context="default recovery session",
                    request_options=self._bounded_cleanup_request_options(),
                )
                self._recovery_session_id = None
            client = self._client
            if client is not None:
                shell_tombstones, bash_tombstones = self._ambiguous_session_creation_snapshot()
                for session_id in shell_tombstones:
                    self._cleanup_session_best_effort(
                        client,
                        session_id,
                        context="ambiguous shell session creation during close",
                        request_options=self._bounded_cleanup_request_options(),
                    )
                for session_id in bash_tombstones:
                    self._cleanup_bash_session_best_effort(
                        client,
                        session_id,
                    )
            # Drop the reference under the lock for use-after-close safety: any
            # later command on this instance fails loudly instead of reusing a
            # half-closed client.
            self._client = None

        if client is None:
            return

        # Walk from the real httpx.Client up to the top-level client, picking the
        # first object that actually exposes close().
        wrapper = getattr(client, "_client_wrapper", None)
        fern_http = getattr(wrapper, "httpx_client", None)
        real_httpx = getattr(fern_http, "httpx_client", None)
        target = next(
            (c for c in (real_httpx, fern_http, client) if c is not None and hasattr(c, "close")),
            None,
        )
        if target is None:
            logger.debug("AioSandbox %s: no closable client found, nothing to release", self.id)
            return

        try:
            target.close()
        except Exception as e:
            logger.warning(f"Error closing AioSandbox client for {self.id}: {e}")

    @staticmethod
    def _cleanup_session_best_effort(
        client,
        session_id: str,
        *,
        context: str,
        request_options: dict[str, int] | None = None,
    ) -> None:
        try:
            client.shell.cleanup_session(
                session_id,
                **({"request_options": request_options} if request_options is not None else {}),
            )
        except Exception as cleanup_error:
            logger.warning(
                "Failed to release shell session %s (%s): %s",
                session_id,
                context,
                cleanup_error,
            )

    @staticmethod
    def _format_shell_result(result) -> tuple[str, int | None, str | None]:
        data = result.data if result else None
        output = data.output if data else ""
        exit_code = getattr(data, "exit_code", None) if data else None
        status = getattr(data, "status", None) if data else None
        return output, exit_code, status

    @staticmethod
    def _is_missing_shell_session_error(error: ApiError) -> bool:
        body = error.body
        if error.status_code != 404 or not isinstance(body, dict):
            return False
        message = body.get("message")
        return isinstance(message, str) and "session not found" in message.casefold()

    def _cleanup_bash_session_best_effort(self, client, session_id: str) -> None:
        try:
            client.bash.close_session(
                session_id,
                request_options=self._bounded_cleanup_request_options(),
            )
        except Exception as cleanup_error:
            logger.warning(
                "Failed to release transient bash session %s: %s",
                session_id,
                cleanup_error,
            )

    @staticmethod
    def _is_definite_session_creation_failure(error: Exception) -> bool:
        if isinstance(error, httpx.ConnectError):
            return True
        if isinstance(error, ApiError):
            return 400 <= error.status_code < 500
        return False

    def _session_creation_state(self, plane: str) -> _SessionCreationState:
        if plane == "shell":
            return self._shell_session_creation_state
        if plane == "bash":
            return self._bash_session_creation_state
        raise ValueError(f"unknown session creation plane: {plane}")

    def _begin_session_creation(self, plane: str, session_id: str) -> None:
        with self._session_creation_state_lock:
            state = self._session_creation_state(plane)
            if state.ambiguous:
                raise RuntimeError(f"AIO {plane} session creation is quarantined after an earlier ambiguous create outcome; recycle the sandbox before creating another session")
            state.pending.add(session_id)

    def _resolve_session_creation(self, plane: str, session_id: str) -> None:
        with self._session_creation_state_lock:
            self._session_creation_state(plane).pending.discard(session_id)

    def _mark_session_creation_ambiguous(
        self,
        plane: str,
        session_id: str,
    ) -> None:
        with self._session_creation_state_lock:
            state = self._session_creation_state(plane)
            state.pending.discard(session_id)
            state.ambiguous.add(session_id)

    def _ambiguous_session_creation_snapshot(
        self,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        with self._session_creation_state_lock:
            return (
                tuple(self._shell_session_creation_state.ambiguous),
                tuple(self._bash_session_creation_state.ambiguous),
            )

    @property
    def requires_container_recycle(self) -> bool:
        with self._session_creation_state_lock:
            shell = self._shell_session_creation_state
            bash = self._bash_session_creation_state
            return bool(shell.pending or shell.ambiguous or bash.pending or bash.ambiguous)

    def _create_shell_session(self, client) -> str:
        session_id = str(uuid.uuid4())
        self._begin_session_creation("shell", session_id)

        try:
            client.shell.create_session(
                id=session_id,
                request_options=self._session_create_request_options(),
            )
        except Exception as error:
            if self._is_definite_session_creation_failure(error):
                self._resolve_session_creation("shell", session_id)
                raise

            self._mark_session_creation_ambiguous("shell", session_id)

            # Best effort only. A successful cleanup does NOT clear the tombstone:
            # the original create may still commit after this cleanup returns.
            self._cleanup_session_best_effort(
                client,
                session_id,
                context="ambiguous shell session creation",
                request_options=self._bounded_cleanup_request_options(),
            )
            raise RuntimeError("AIO shell session creation outcome is unknown; the sandbox is quarantined for recycle") from error

        self._resolve_session_creation("shell", session_id)
        return session_id

    def _create_bash_session(self, client) -> str:
        session_id = str(uuid.uuid4())
        self._begin_session_creation("bash", session_id)

        try:
            client.bash.create_session(
                session_id=session_id,
                request_options=self._session_create_request_options(),
            )
        except Exception as error:
            if self._is_definite_session_creation_failure(error):
                self._resolve_session_creation("bash", session_id)
                raise

            self._mark_session_creation_ambiguous("bash", session_id)

            # Same rule as shell: compensation is bounded but cannot prove that
            # the original create will not commit later.
            self._cleanup_bash_session_best_effort(
                client,
                session_id,
            )
            raise RuntimeError("AIO bash session creation outcome is unknown; the sandbox is quarantined for recycle") from error

        self._resolve_session_creation("bash", session_id)
        return session_id

    def _ensure_default_shell_session_id(self, client) -> str:
        """Return the session id that may safely receive default-shell work.

        Omitting an id creates a new session on the AIO API. Use an explicit id
        from the first command, both for persistence and cancellation targeting.

        Caller must hold ``self._lock``.
        """
        if self._recovery_session_id is None:
            self._recovery_session_id = self._create_shell_session(client)
        return self._recovery_session_id

    def _exec_shell(
        self,
        client,
        command: str,
        *,
        session_id: str,
        timeout: float,
    ) -> tuple[str, int | None, str | None]:
        with self._abort_state_lock():
            token = self._session_abort_tokens.get(session_id)
        marking = token is None
        if marking:
            # The first command this object sends a session also exports the
            # session's abort token, so every process the session starts from
            # here on carries it, including one that detaches itself. A fresh
            # session has no state for the prefix to disturb; no later command
            # is touched.
            token = f"df-{uuid.uuid4().hex}"
            sent = _shell_marking_prefix(token) + command
        else:
            sent = command
        protected = frozenset() if marking else self._snapshot_existing_abort_processes(client, token)
        kwargs = {
            # /v1/shell is a persistent PTY. Keep its command and terminal stdin
            # intact; the broker shim already treats a TTY as non-payload input.
            "command": sent,
            "no_change_timeout": self._effective_no_change_timeout(timeout),
            "hard_timeout": timeout,
            "async_mode": True,
            "request_options": self._command_request_options(timeout),
        }
        if session_id is not None:
            kwargs["id"] = session_id
        with self._command_in_flight(session_id, token, started=time.monotonic(), protected_processes=protected) as inflight:
            try:
                deadline = time.monotonic() + timeout + 5
                viewed = False
                accepted = False
                result = client.shell.exec_command(**kwargs)
                while getattr(getattr(result, "data", None), "status", None) == "running":
                    accepted = True
                    if inflight.aborted:
                        return "Error: command cancelled", None, "terminated"
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise httpx.ReadTimeout("AIO shell command response deadline exceeded")
                    options = {"timeout_in_seconds": min(2, remaining), "max_retries": 0}
                    client.shell.wait_for_process(id=session_id, max_wait_seconds=1, request_options=options)
                    if inflight.aborted:
                        return "Error: command cancelled", None, "terminated"
                    result = client.shell.view(id=session_id, request_options=options)
                    viewed = True
            except ApiError as error:
                if inflight.aborted:
                    return "Error: command cancelled", None, "terminated"
                if accepted:
                    # An accepted command may have already changed files. Route
                    # lost polling responses through the no-replay transport
                    # fence rather than the missing-session dispatch recovery.
                    raise httpx.ReadError("AIO accepted command outcome is unknown after polling failed") from error
                raise
            if inflight.aborted:
                return "Error: command cancelled", None, "terminated"
        with self._abort_state_lock():
            self._remember_session_token(session_id, token)
        if viewed:
            console = getattr(getattr(result, "data", None), "console", None)
            if console:
                _, exit_code, status = self._format_shell_result(result)
                return console[-1].output, exit_code, status
        return self._format_shell_result(result)

    def _snapshot_existing_abort_processes(self, client, token: str) -> frozenset[tuple[int, int]]:
        """Identify older jobs exactly, including ones started moments earlier.

        Host/container clock rounding cannot distinguish adjacent calls. Read
        identities on a separate shell so the user's command stays unchanged.
        """
        session_id = self._create_shell_session(client)
        options = self._bounded_cleanup_request_options()
        marker = shlex.quote(f"{ABORT_TOKEN_ENV}={token}")
        command = f'printf \'DF_ABORT_SNAPSHOT \'; {_ABORT_SWEEP_HEAD}{marker}; then if read -r s < "$d/stat" 2>/dev/null; then s=${{s##*) }}; set -- $s; printf "%s:%s " "$p" "${{20}}"; fi; fi; done; printf "\\n"'
        try:
            result = client.shell.exec_command(command=command, id=session_id, no_change_timeout=1, request_options=options)
            output, exit_code, status = self._format_shell_result(result)
            parts = output.split()
            if status not in (None, "completed") or exit_code not in (None, 0) or not parts or parts[0] != "DF_ABORT_SNAPSHOT":
                raise RuntimeError("Could not identify existing sandbox background jobs")
            return frozenset((int(pid), int(ticks)) for pid, ticks in (identity.split(":") for identity in parts[1:]))
        finally:
            self._cleanup_session_best_effort(client, session_id, context="background job snapshot", request_options=options)

    def _abort_state_lock(self) -> threading.Lock:
        """The lock guarding the abort state, creating that state where ``__init__`` did not run.

        A sandbox object can be built without its constructor (a caller that
        fills in only what it uses). Executing a command on one must not fail
        for want of bookkeeping that exists only so a command can be stopped.
        """
        state = self.__dict__
        lock = state.get("_abort_lock")
        if lock is None:
            state.setdefault("_inflight_commands", {})
            state.setdefault("_next_command_seq", 0)
            state.setdefault("_session_abort_tokens", {})
            state.setdefault("_aborted_calls", {})
            lock = state.setdefault("_abort_lock", threading.Lock())
        return lock

    def _remember_session_token(self, session_id: str, token: str) -> None:
        """Record an explicit session's token. Caller holds ``_abort_lock``.

        Sessions come and go with every subagent and nothing here sees one
        end, so the memory is bounded. Forgetting a live session only means it
        is marked again on its next command.
        """
        self._session_abort_tokens[session_id] = token
        while len(self._session_abort_tokens) > _SESSION_TOKEN_MEMORY:
            self._session_abort_tokens.pop(next(iter(self._session_abort_tokens)))

    @contextlib.contextmanager
    def _command_in_flight(self, session_id: str | None, token: str, *, started: float | None = None, protected_processes: frozenset[tuple[int, int]] | None = None):
        """Hold one command open for as long as it is executing in the container.

        Shell commands have explicit session ids before dispatch. Env-bearing
        bash.exec commands use a per-command token rather than a shell identity.
        The token marks the processes the command starts: every
        child inherits it, including one that detaches itself. Persistent
        shells protect exact identities captured before dispatch. ``started``
        remains the fallback for commands without a shell snapshot.
        """
        call_id = current_sandbox_command_call()
        with self._abort_state_lock():
            if call_id is not None and call_id in self._aborted_calls:
                raise RuntimeError("sandbox command call was cancelled before execution")
            self._next_command_seq += 1
            sequence = self._next_command_seq
            inflight = _InflightCommand(call_id, session_id, token, started, protected_processes)
            self._inflight_commands[sequence] = inflight
        try:
            yield inflight
        finally:
            if inflight.aborted and not inflight.abort_complete.wait(timeout=inflight.abort_wait_timeout):
                logger.warning("Abort control exceeded its cleanup budget for sandbox %s", self.id)
            with self._abort_state_lock():
                self._inflight_commands.pop(sequence, None)

    def _call_was_aborted(self) -> bool:
        """Whether the call running here has already been told to stop."""
        call_id = current_sandbox_command_call()
        if call_id is None:
            return False
        with self._abort_state_lock():
            return call_id in self._aborted_calls

    def _rotate_and_retry_shell(
        self,
        client,
        command: str,
        *,
        corrupted_session_id: str | None,
        context: str,
        timeout: float,
    ) -> tuple[str, int | None, str | None, str | None]:
        cleanup_options = self._bounded_cleanup_request_options()
        if corrupted_session_id is not None:
            self._cleanup_session_best_effort(
                client,
                corrupted_session_id,
                context=f"corrupted {context}",
                request_options=cleanup_options,
            )
        replacement_id = self._create_shell_session(client)
        try:
            output, exit_code, status = self._exec_shell(
                client,
                command,
                session_id=replacement_id,
                timeout=timeout,
            )
        except BaseException:
            self._cleanup_session_best_effort(
                client,
                replacement_id,
                context=f"abandoned replacement for {context}",
                request_options=cleanup_options,
            )
            raise
        if self._is_session_invalidating_shell_status(status):
            if status == "terminated":
                cleanup_context = f"terminated replacement for {context}"
            elif status == "no_change_timeout":
                cleanup_context = f"ambiguous replacement for {context}"
            else:
                cleanup_context = f"unexpected replacement status for {context}"
            self._cleanup_session_best_effort(
                client,
                replacement_id,
                context=cleanup_context,
                request_options=cleanup_options,
            )
            return output, exit_code, status, None
        if status in (None, "completed") and output and _ERROR_OBSERVATION_SIGNATURE in output:
            self._cleanup_session_best_effort(
                client,
                replacement_id,
                context=f"failed replacement for {context}",
                request_options=cleanup_options,
            )
            return output, exit_code, status, None
        return output, exit_code, status, replacement_id

    def abort_running_commands(self, call_id: str | None = None) -> int:
        """Kill one call's processes in the container and return how many commands it had.

        Called from a worker thread while the command being ended still holds
        ``_lock`` inside its own HTTP request, so this path takes no lock that
        command holds and never queues behind it.

        Kill the parent shell first: killing only its foreground child lets
        Bash continue with the rest of the submitted command. Then sweep the
        command's descendants, including detached ones, while sparing older
        jobs. Shorten the PTY's idle wait so its pending HTTP request drains.
        Every request is bounded and the cancelled generation is discarded.

        ``call_id`` selects one tool call's commands, because one sandbox is
        shared by the lead agent and its subagents; ``None`` means every
        command in the sandbox.

        Returns the number of commands that were executing. Zero means there
        was nothing to end and nothing is asked of the container.
        """
        with self._abort_state_lock():
            if call_id is not None:
                self._aborted_calls[call_id] = None
                while len(self._aborted_calls) > _ABORTED_CALL_MEMORY:
                    self._aborted_calls.pop(next(iter(self._aborted_calls)))
            selected = [command for command in self._inflight_commands.values() if call_id is None or command.call_id == call_id]
            for command in selected:
                # Returning early would let ordinary session cleanup kill the
                # foreground child before the abort control kills its parent.
                command.abort_wait_timeout = self._ABORT_REQUEST_TIMEOUT_SECONDS * (6 + 8 * len(selected))
                command.aborted = True
        if not selected:
            return 0
        try:
            client = self._client
            if client is None:
                return 0
            options = {
                "timeout_in_seconds": self._ABORT_REQUEST_TIMEOUT_SECONDS,
                "max_retries": 0,
            }
            for session_id in sorted({command.session_id for command in selected if command.session_id is not None}):
                try:
                    client.shell.update_session(id=session_id, no_change_timeout=1, request_options=options)
                except Exception:
                    logger.warning(
                        "Failed to shorten the drain of shell session %s of sandbox %s",
                        session_id,
                        self.id,
                        exc_info=True,
                    )
            self._sweep_processes_carrying_the_abort_tokens(client, selected, options)
            for session_id in sorted({command.session_id for command in selected if command.session_id is not None}):
                # Cleanup also removes a generation whose exec request had not
                # arrived when the abort began. A later 404 must never replay it.
                self._cleanup_session_best_effort(client, session_id, context="cancelled command", request_options=options)
            logger.info("Sandbox %s aborted %d running command(s)", self.id, len(selected))
            return len(selected)
        finally:
            for command in selected:
                command.abort_complete.set()

    def _sweep_processes_carrying_the_abort_tokens(self, client, commands: list[_InflightCommand], options: dict[str, int]) -> None:
        """Kill the processes in the container that these commands started.

        Runs on its own fresh shell session so it does not queue behind the
        command it is ending, and cleans that session up afterwards. Every
        request carries the abort's own timeout, never the client's 600 s
        command budget: an abort that cannot be delivered has to give up and
        leave the command's own timeout as the fallback, not become a second
        hang in front of the drain. A failure here is logged and not raised --
        cleanup still discards the cancelled session if this sweep fails.
        """
        session_id: str | None = None
        try:
            session_id = str(uuid.uuid4())
            client.shell.create_session(id=session_id, request_options=options)
            # One sweep per token. A session runs one command at a time, so a
            # token normally has one command here; should it ever have two,
            # the older one bounds the sweep.
            sweeps: dict[str, float | None] = {}
            for command in commands:
                if command.token not in sweeps:
                    sweeps[command.token] = command.started
                elif sweeps[command.token] is not None and command.started is not None:
                    sweeps[command.token] = min(sweeps[command.token], command.started)
                else:
                    sweeps[command.token] = None
            for token in sorted(sweeps):
                started = sweeps[token]
                if started is not None:
                    client.shell.exec_command(
                        command=_abort_shell_command(token),
                        no_change_timeout=self._ABORT_NO_CHANGE_TIMEOUT,
                        id=session_id,
                        request_options=options,
                    )
                    # Wake AIO's waiter before the descendant sweep removes its
                    # foreground process. Afterwards AIO can select an older
                    # background job, and the pending exec keeps its idle wait.
                    for target in sorted({command.session_id for command in commands if command.token == token and command.session_id is not None}):
                        try:
                            client.shell.kill_process(id=target, request_options=options)
                        except Exception:
                            logger.warning("Failed to wake the cancelled shell session %s of sandbox %s", target, self.id, exc_info=True)
                # Measured immediately before the request, so the allowance
                # only has to cover the request itself.
                within = None if started is None else int(time.monotonic() - started) + _ABORT_SWEEP_AGE_ALLOWANCE_SECONDS
                client.shell.exec_command(
                    command=_abort_sweep_command(token, started_within_seconds=within, protected_processes=next(command.protected_processes for command in commands if command.token == token)),
                    no_change_timeout=self._ABORT_NO_CHANGE_TIMEOUT,
                    id=session_id,
                    request_options=options,
                )
        except Exception:
            logger.warning(
                "Abort sweep failed in sandbox %s; detached processes may survive",
                self.id,
                exc_info=True,
            )
        finally:
            if session_id is not None:
                self._cleanup_session_best_effort(client, session_id, context="abort sweep", request_options=options)

    def execute_command_in_scope(
        self,
        command: str,
        env: dict[str, str] | None = None,
        timeout: float | None = None,
        *,
        scope_id: str | None = None,
    ) -> str:
        """Run no-env commands in one persistent session per subagent run.

        Commands within a scope remain serialized, while independent subagents
        use distinct server-side sessions and can execute concurrently. Secret-
        bearing commands keep the existing fresh ``bash.exec`` behavior.
        """
        if env or scope_id is None:
            return self.execute_command(command, env=env, timeout=timeout)
        _validate_extra_env(env)

        try:
            with self._scope_registry_lock:
                if self._closed:
                    raise RuntimeError("sandbox client is closed")
                scoped = self._scoped_shell_sessions.setdefault(
                    scope_id,
                    _ScopedShellSession(),
                )
            with scoped.lock:
                # Registration and command execution are separated by the
                # per-scope wait.  Revalidate identity after that wait so a
                # release/close which removed this exact scope is a hard
                # lifecycle fence: queued callers cannot resurrect a session
                # on an orphaned registry entry.
                with self._scope_registry_lock:
                    if self._closed or self._scoped_shell_sessions.get(scope_id) is not scoped:
                        raise RuntimeError("sandbox command scope is no longer active")
                client = self._client
                if client is None:
                    raise RuntimeError("sandbox client is closed")
                if scoped.session_id is None:
                    scoped.session_id = self._create_shell_session(client)
                try:
                    effective_timeout = self._effective_command_timeout(timeout)
                    output, exit_code, status = self._exec_shell(
                        client,
                        command,
                        session_id=scoped.session_id,
                        timeout=effective_timeout,
                    )
                except httpx.TransportError as exc:
                    session_id = scoped.session_id
                    scoped.session_id = None
                    if session_id is not None:
                        self._cleanup_session_best_effort(
                            client,
                            session_id,
                            context="execution scope after transport failure",
                            request_options=self._bounded_cleanup_request_options(),
                        )
                    return self._transport_failure_error(exc, effective_timeout)
                except ApiError as error:
                    if not self._is_missing_shell_session_error(error):
                        raise
                    logger.warning("Execution-scoped sandbox shell session is missing; recreating it once")
                    scoped.session_id = None
                    try:
                        output, exit_code, status, scoped.session_id = self._rotate_and_retry_shell(
                            client,
                            command,
                            corrupted_session_id=None,
                            context="execution scope after missing session",
                            timeout=effective_timeout,
                        )
                    except httpx.TransportError as exc:
                        return self._transport_failure_error(exc, effective_timeout)
                if self._is_session_invalidating_shell_status(status):
                    session_id = scoped.session_id
                    scoped.session_id = None
                    if session_id is not None:
                        if status == "terminated":
                            cleanup_context = "execution scope after terminal session loss"
                        elif status == "no_change_timeout":
                            cleanup_context = "execution scope after ambiguous no-change timeout"
                        else:
                            cleanup_context = "execution scope after unexpected status"
                        self._cleanup_session_best_effort(
                            client,
                            session_id,
                            context=cleanup_context,
                            request_options=self._bounded_cleanup_request_options(),
                        )
                if scoped.session_id is not None and status in (None, "completed") and output and _ERROR_OBSERVATION_SIGNATURE in output and self._call_was_aborted():
                    logger.info("Sandbox %s left an aborted call's scoped command stopped rather than retrying it", self.id)
                elif scoped.session_id is not None and status in (None, "completed") and output and _ERROR_OBSERVATION_SIGNATURE in output:
                    logger.warning("ErrorObservation detected in sandbox output for execution scope; rotating session")
                    corrupted_session_id = scoped.session_id
                    scoped.session_id = None
                    try:
                        output, exit_code, status, scoped.session_id = self._rotate_and_retry_shell(
                            client,
                            command,
                            corrupted_session_id=corrupted_session_id,
                            context="execution scope",
                            timeout=effective_timeout,
                        )
                    except httpx.TransportError as exc:
                        return self._transport_failure_error(exc, effective_timeout)
                return self._render_shell_output(
                    output,
                    exit_code,
                    status=status,
                    timeout=effective_timeout,
                )
        except Exception as e:
            logger.error(f"Failed to execute command in sandbox: {e}")
            return f"Error: {e}"

    def release_command_scope(self, scope_id: str) -> None:
        """Clean up one subagent's explicit server-side shell session."""
        with self._scope_registry_lock:
            scoped = self._scoped_shell_sessions.pop(scope_id, None)
        if scoped is None:
            return
        with scoped.lock:
            if scoped.session_id is None or self._client is None:
                return
            self._cleanup_session_best_effort(
                self._client,
                scoped.session_id,
                context=f"execution scope {scope_id}",
                request_options=self._bounded_cleanup_request_options(),
            )
            scoped.session_id = None

    @staticmethod
    def _format_timeout_duration(timeout: float) -> str:
        return f"{timeout:g}"

    @classmethod
    def _format_timeout_notice(cls, timeout: float) -> str:
        return f"Command timed out after {cls._format_timeout_duration(timeout)} seconds and was terminated."

    @classmethod
    def _bounded_cleanup_request_options(cls) -> dict[str, int]:
        return {
            "timeout_in_seconds": cls._CLEANUP_REQUEST_TIMEOUT_SECONDS,
            "max_retries": 0,
        }

    @classmethod
    def _session_create_request_options(cls) -> dict[str, int]:
        return {
            "timeout_in_seconds": cls._SESSION_CREATE_REQUEST_TIMEOUT_SECONDS,
            "max_retries": 0,
        }

    @classmethod
    def _transport_timeout_error(cls, timeout: float) -> str:
        request_timeout = cls._command_request_options(timeout)["timeout_in_seconds"]
        return f"Error: Sandbox command response timed out after {request_timeout} seconds; command outcome is unknown and the command was not retried."

    @classmethod
    def _transport_failure_error(cls, error: httpx.TransportError, timeout: float) -> str:
        if isinstance(error, httpx.TimeoutException):
            return cls._transport_timeout_error(timeout)
        return "Error: Sandbox command transport failed; command outcome is unknown and the command was not retried."

    @staticmethod
    def _is_unexpected_shell_status(status: str | None) -> bool:
        return status not in (None, "completed", "hard_timeout", "no_change_timeout", "terminated")

    @classmethod
    def _is_session_invalidating_shell_status(cls, status: str | None) -> bool:
        return status in ("terminated", "no_change_timeout") or cls._is_unexpected_shell_status(status)

    @classmethod
    def _unexpected_status_notice(cls, status: str) -> str:
        return f"Error: Sandbox command returned an unexpected status '{status}'; command outcome is unknown and the command was not retried."

    @classmethod
    def _render_shell_output(
        cls,
        output: str,
        exit_code: int | None,
        *,
        status: str | None,
        timeout: float,
    ) -> str:
        if status == "hard_timeout":
            notice = cls._format_timeout_notice(timeout)
            output = f"{output}\n{notice}" if output else notice
            return f"{output}\nExit Code: 124"

        if status == "no_change_timeout":
            effective_no_change_timeout = cls._effective_no_change_timeout(timeout)
            notice = f"Command produced no output change for {effective_no_change_timeout} seconds; it may still be running. Command outcome is unknown and was not retried."
            return f"{output}\n{notice}" if output else notice

        if status == "terminated":
            notice = "Command was terminated because its shell session ended."
            return f"{output}\n{notice}" if output else notice

        if cls._is_unexpected_shell_status(status):
            notice = cls._unexpected_status_notice(status)
            return f"{output}\n{notice}" if output else notice

        if exit_code not in (0, None):
            output = f"{output}\nExit Code: {exit_code}" if output else f"Command exited with code {exit_code}"
        return output if output else "(no output)"

    @property
    def home_dir(self) -> str:
        """Get the home directory inside the sandbox."""
        if self._home_dir is None:
            context = self._client.sandbox.get_context()
            self._home_dir = context.home_dir
        return self._home_dir

    # Default no_change_timeout base idle guard for exec_command (seconds).
    # The per-command effective value is max(600, ceil(T + 5)); at the default
    # T=600, the value sent to the sandbox is therefore 605, not 600.
    _DEFAULT_NO_CHANGE_TIMEOUT = 600

    # Fallback command hard timeout for both the legacy shell path and
    # env-bearing commands routed through bash.exec when no provider default is set.
    # The bash.exec API exposes no idle/no-change timeout (unlike
    # shell.exec_command's ``no_change_timeout`` on the legacy path), so
    # env-bearing commands are bounded by total elapsed wall-clock time, not
    # time-since-last-output. Kept at the same numeric value as the legacy idle
    # budget so the two paths broadly agree on how long a single command may
    # run; a future SDK that exposes an idle timeout on bash.exec should switch
    # this call site to it.
    _DEFAULT_HARD_TIMEOUT = 600.0
    _REQUEST_TIMEOUT_GRACE_SECONDS = 5.0
    _CLEANUP_REQUEST_TIMEOUT_SECONDS = 5
    _SESSION_CREATE_REQUEST_TIMEOUT_SECONDS = 5

    # Directory-operation deadline for ``list_dir`` (#5644). ``list_dir`` is an
    # independent operation, not a shell command: it must not inherit
    # ``bash_command_timeout`` (600s default), or a wedged ``find`` holds
    # ``self._lock`` for the full SDK budget. 60s is far above a real
    # ``max_depth=2`` traversal and far below the SDK's 600s, and stays a
    # private constant rather than new operator config.
    _LIST_DIR_TIMEOUT_SECONDS = 60.0

    def _effective_command_timeout(self, timeout: float | None) -> float:
        return timeout if timeout is not None else (getattr(self, "_default_command_timeout", None) or self._DEFAULT_HARD_TIMEOUT)

    @classmethod
    def _effective_no_change_timeout(cls, timeout: float) -> int:
        return max(
            cls._DEFAULT_NO_CHANGE_TIMEOUT,
            math.ceil(timeout + cls._REQUEST_TIMEOUT_GRACE_SECONDS),
        )

    @classmethod
    def _command_request_options(cls, timeout: float) -> dict[str, int]:
        return {
            "timeout_in_seconds": max(
                1,
                math.ceil(timeout + cls._REQUEST_TIMEOUT_GRACE_SECONDS),
            ),
            "max_retries": 0,
        }

    # The abort sweep walks /proc and sends signals; it either finishes in a
    # moment or the container is not answering. It must never inherit the
    # 600 s command budget, because the thread waiting on the cancelled tool
    # call is what the bound is really about.
    _ABORT_NO_CHANGE_TIMEOUT = 20

    # HTTP bound for every abort request, separate from the client's 600 s
    # command timeout: an abort that cannot be delivered has to give up so the
    # command's own timeout remains the fallback rather than a second hang.
    _ABORT_REQUEST_TIMEOUT_SECONDS = 20

    def execute_command(
        self,
        command: str,
        env: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> str:
        """Execute a shell command in the sandbox.

        Uses a lock to serialize unscoped requests. The AIO sandbox container's
        implicit persistent shell corrupts when hit with concurrent
        ``exec_command`` calls (returning ``ErrorObservation`` instead of real
        output). If corruption is detected despite the lock (e.g. multiple
        processes sharing a sandbox), the replacement session is promoted for
        subsequent calls rather than returning to the corrupted implicit one.

        Args:
            command: The command to execute.
            env: Optional per-call environment variables (request-scoped secrets,
                issue #3861). When provided, the command runs via the ``bash.exec``
                API (which supports per-command env) on a fresh explicitly released
                session, so the secrets are scoped to this single command and never
                persist; secret values travel in the structured ``env`` field, never
                in the command string. When ``None``, the legacy persistent-shell
                path still uses the provider-wide command timeout via
                ``hard_timeout``, bounded request options, and returned-status
                interpretation.
            timeout: Optional per-call command timeout. The legacy shell path
                enforces it server-side and gives the request a small additional
                response-path grace period.

        Returns:
            The output of the command.
        """
        # Validate ``env`` keys before forwarding them to the ``bash.exec`` API.
        # The public ``Sandbox.execute_command`` contract accepts arbitrary dict
        # keys; enforcing the POSIX env-var name rule keeps the contract
        # consistent with the local and e2b sandboxes and catches unsafe keys
        # early. ``_validate_extra_env`` is a no-op when ``env`` is None or empty.
        _validate_extra_env(env)
        effective_timeout = self._effective_command_timeout(timeout)
        if env:
            return self._execute_with_env(command, env, effective_timeout)
        with self._lock:
            try:
                client = self._client
                if getattr(self, "_closed", False) or client is None:
                    raise RuntimeError("sandbox client is closed")
                session_id = self._ensure_default_shell_session_id(client)
                recovered_missing_session = False
                try:
                    output, exit_code, status = self._exec_shell(
                        client,
                        command,
                        session_id=session_id,
                        timeout=effective_timeout,
                    )
                except httpx.TransportError as exc:
                    session_id = self._recovery_session_id
                    self._recovery_session_id = None
                    self._default_shell_corrupted = True
                    if session_id is not None:
                        self._cleanup_session_best_effort(
                            client,
                            session_id,
                            context="default shell after transport failure",
                            request_options=self._bounded_cleanup_request_options(),
                        )
                    return self._transport_failure_error(exc, effective_timeout)
                except ApiError as error:
                    if not self._is_missing_shell_session_error(error):
                        raise
                    logger.warning("Default sandbox shell session is missing; recreating it once")
                    self._default_shell_corrupted = True
                    self._recovery_session_id = None
                    recovered_missing_session = True
                    try:
                        output, exit_code, status, self._recovery_session_id = self._rotate_and_retry_shell(
                            client,
                            command,
                            corrupted_session_id=None,
                            context="default shell after missing session",
                            timeout=effective_timeout,
                        )
                    except httpx.TransportError as exc:
                        return self._transport_failure_error(exc, effective_timeout)

                if not recovered_missing_session and status in (None, "completed") and output and _ERROR_OBSERVATION_SIGNATURE in output and self._call_was_aborted():
                    # The abort killed this command on purpose. Rotating would
                    # re-run it on a fresh session, so the work that was ended
                    # would start again and run to its own budget.
                    logger.info("Sandbox %s left an aborted call's command stopped rather than retrying it", self.id)
                elif not recovered_missing_session and status in (None, "completed") and output and _ERROR_OBSERVATION_SIGNATURE in output:
                    self._default_shell_corrupted = True
                    logger.warning("ErrorObservation detected in sandbox output, retrying on a fresh session")
                    corrupted_session_id = self._recovery_session_id
                    self._recovery_session_id = None
                    try:
                        output, exit_code, status, self._recovery_session_id = self._rotate_and_retry_shell(
                            client,
                            command,
                            corrupted_session_id=corrupted_session_id,
                            context="default shell",
                            timeout=effective_timeout,
                        )
                    except httpx.TransportError as exc:
                        return self._transport_failure_error(exc, effective_timeout)

                if self._is_session_invalidating_shell_status(status):
                    session_id = self._recovery_session_id
                    self._recovery_session_id = None
                    self._default_shell_corrupted = True
                    if session_id is not None:
                        if status == "terminated":
                            cleanup_context = "default shell after terminal session loss"
                        elif status == "no_change_timeout":
                            cleanup_context = "default shell after ambiguous no-change timeout"
                        else:
                            cleanup_context = "default shell after unexpected status"
                        self._cleanup_session_best_effort(
                            client,
                            session_id,
                            context=cleanup_context,
                            request_options=self._bounded_cleanup_request_options(),
                        )

                return self._render_shell_output(
                    output,
                    exit_code,
                    status=status,
                    timeout=effective_timeout,
                )
            except Exception as e:
                logger.error(f"Failed to execute command in sandbox: {e}")
                return f"Error: {e}"

    def _execute_with_env(
        self,
        command: str,
        env: dict[str, str],
        timeout: float,
    ) -> str:
        """Execute a command with per-call environment variables injected.

        The persistent-shell ``shell.exec_command`` API has no env parameter, so
        injected commands use the ``bash.exec`` API which accepts per-command env.
        Each call creates an explicit transient session and closes it after the
        command, so injected request-scoped secrets are scoped to this command,
        never persist across calls, and do not consume the server's session
        capacity after completion. Secret values travel in the structured
        ``env`` field, never in the command string.

        Trade-off of the fresh-session choice: consecutive env-bearing bash calls
        within the same skill do not share session state (cwd, sourced venv,
        exported variables). This mirrors the LocalSandbox model (each call is a
        fresh subprocess) and is intentional — a shared session_id would let
        request-scoped secrets ride the session env into later commands, which the
        SDK does not contractually forbid. Skills that need setup must fold it into
        a single command (e.g. ``cd /mnt/user-data/workspace && source .venv/bin/activate && python run.py``).

        The ``_ERROR_OBSERVATION_SIGNATURE`` recovery contract is shared with the
        legacy persistent-shell path: if the (unlikely, since each call is a fresh
        session) corruption marker shows up, the call is retried on another fresh
        session rather than returned verbatim.

        Images older than all-in-one-sandbox 1.9.x have no ``/v1/bash/*`` routes;
        there is no fallback on the legacy shell path that would keep the secret
        values out of the command string, so the only safe behaviour is to fail
        fast with an actionable error (#3921).
        """
        if self._bash_exec_unsupported:
            return _BASH_EXEC_UNSUPPORTED_ERROR
        output, status = self._run_bash_exec(command, env, timeout)
        if status in (None, "completed") and output and _ERROR_OBSERVATION_SIGNATURE in output and self._call_was_aborted():
            logger.info("Sandbox %s left an aborted call's command stopped rather than retrying it", self.id)
        elif status in (None, "completed") and output and _ERROR_OBSERVATION_SIGNATURE in output:
            logger.warning("ErrorObservation detected in bash.exec output, retrying on a fresh session")
            retried, retry_status = self._run_bash_exec(command, env, timeout)
            if retry_status not in (None, "completed"):
                return retried
            if retried and _ERROR_OBSERVATION_SIGNATURE not in retried:
                return retried
        return output

    def _run_bash_exec(
        self,
        command: str,
        env: dict[str, str],
        timeout: float,
    ) -> tuple[str, str | None]:
        """Single bash.exec invocation in an explicitly released fresh session."""
        with self._lock:
            for attempt in range(2):
                session_id: str | None = None
                try:
                    session_id = self._create_bash_session(self._client)
                    # The abort token rides the structured env, where the
                    # secrets already are, rather than the command string.
                    # Counting this call is what makes it abortable at all.
                    token = f"df-{uuid.uuid4().hex}"
                    with self._command_in_flight(None, token):
                        result = self._client.bash.exec(
                            # /v1/bash keeps a subprocess stdin pipe open for writes.
                            # This fresh, released session is non-interactive, so close
                            # its default input before running the original script.
                            # Explicit pipes/heredocs/files still override fd0. A plain
                            # prefix keeps top-level parsing (aliases/extglob) and never
                            # appends a delimiter that a trailing backslash can consume.
                            # Do not apply exec to the persistent PTY transport above.
                            command=f"exec < /dev/null\n{command}",
                            session_id=session_id,
                            env={**env, ABORT_TOKEN_ENV: token},
                            hard_timeout=timeout,
                            request_options=self._command_request_options(timeout),
                        )
                    data = result.data if result else None
                    stdout = (data.stdout or "") if data else ""
                    stderr = (data.stderr or "") if data else ""
                    exit_code = getattr(data, "exit_code", None) if data else None
                    status = getattr(data, "status", None) if data else None
                    output = stdout
                    if stderr:
                        output += f"\nStd Error:\n{stderr}" if output else stderr

                    if status == "timed_out":
                        notice = self._format_timeout_notice(timeout)
                        output = f"{output}\n{notice}" if output else notice
                        return f"{output}\nExit Code: 124", status

                    if status == "killed":
                        notice = "Command was killed before completion."
                        output = f"{output}\n{notice}" if output else notice
                        return output, status

                    if status == "running":
                        notice = "Error: Sandbox command returned a non-terminal running status; command outcome is unknown and was not retried."
                        output = f"{output}\n{notice}" if output else notice
                        return output, status

                    if status not in (None, "completed"):
                        notice = self._unexpected_status_notice(status)
                        output = f"{output}\n{notice}" if output else notice
                        return output, status

                    if exit_code not in (0, None):
                        # Mirror LocalSandbox: keep the actual shell status in the
                        # output text (acceptance-checklist evidence).
                        output = f"{output}\nExit Code: {exit_code}" if output else f"Command exited with code {exit_code}"
                    return output if output else "(no output)", status
                except httpx.TimeoutException:
                    return self._transport_timeout_error(timeout), "transport_timeout"
                except ApiError as e:
                    if self._is_missing_shell_session_error(e):
                        if attempt == 0:
                            logger.warning("Transient bash.exec session disappeared; retrying once")
                            continue
                        logger.error("Failed to execute command with injected env: bash.exec session disappeared after retry")
                        return "Error: bash.exec session disappeared after retry", None
                    if e.status_code == 404:
                        self._bash_exec_unsupported = True
                        logger.error("Sandbox %s does not support bash.exec (/v1/bash/exec returned 404); env-bearing commands are unavailable until the sandbox image is upgraded to all-in-one-sandbox >= 1.9.3", self.id)
                        return _BASH_EXEC_UNSUPPORTED_ERROR, None
                    logger.error(f"Failed to execute command with injected env in sandbox: {e}")
                    return f"Error: {e}", None
                except Exception as e:
                    logger.error(f"Failed to execute command with injected env in sandbox: {e}")
                    return f"Error: {e}", None
                finally:
                    if session_id is not None:
                        self._cleanup_bash_session_best_effort(self._client, session_id)
            return "Error: bash.exec session disappeared after retry", None

    def read_file(
        self,
        path: str,
        start_line: int | None = None,
        end_line: int | None = None,
    ) -> str:
        """Read the content of a file in the sandbox.

        Args:
            path: The absolute path of the file to read.

        Returns:
            The content of the file.
        """
        try:
            kwargs = {}
            if start_line is not None:
                kwargs["start_line"] = max(start_line - 1, 0)
            if end_line is not None:
                kwargs["end_line"] = max(end_line, 0)
            result = self._client.file.read_file(file=path, **kwargs)
            return result.data.content if result.data else ""
        except Exception as e:
            logger.error(f"Failed to read file in sandbox: {e}")
            return f"Error: {e}"

    def download_file(self, path: str) -> bytes:
        """Download file bytes from the sandbox.

        Raises:
            PermissionError: If the path contains '..' traversal segments or is
                outside ``VIRTUAL_PATH_PREFIX``.
            OSError: If the file cannot be retrieved from the sandbox.
        """
        # Reject path traversal before sending to the container API.
        # LocalSandbox gets this implicitly via _resolve_path;
        # here the path is forwarded verbatim so we must check explicitly.
        normalised = path.replace("\\", "/")
        for segment in normalised.split("/"):
            if segment == "..":
                logger.error(f"Refused download due to path traversal: {path}")
                raise PermissionError(f"Access denied: path traversal detected in '{path}'")

        stripped_path = normalised.lstrip("/")
        allowed_prefix = VIRTUAL_PATH_PREFIX.lstrip("/")
        if stripped_path != allowed_prefix and not stripped_path.startswith(f"{allowed_prefix}/"):
            logger.error("Refused download outside allowed directory: path=%s, allowed_prefix=%s", path, VIRTUAL_PATH_PREFIX)
            raise PermissionError(f"Access denied: path must be under '{VIRTUAL_PATH_PREFIX}': '{path}'")

        with self._lock:
            try:
                chunks: list[bytes] = []
                total = 0
                for chunk in self._client.file.download_file(path=path):
                    total += len(chunk)
                    if total > _MAX_DOWNLOAD_SIZE:
                        raise OSError(
                            errno.EFBIG,
                            f"File exceeds maximum download size of {_MAX_DOWNLOAD_SIZE} bytes",
                            path,
                        )
                    chunks.append(chunk)
                return b"".join(chunks)
            except OSError:
                raise
            except Exception as e:
                logger.error(f"Failed to download file in sandbox: {e}")
                raise OSError(f"Failed to download file '{path}' from sandbox: {e}") from e

    def list_dir(self, path: str, max_depth: int = 2) -> list[str]:
        """List the contents of a directory in the sandbox.

        Args:
            path: The absolute path of the directory to list.
            max_depth: The maximum depth to traverse. Default is 2.

        Returns:
            The contents of the directory.
        """
        resolved = path
        timeout = self._LIST_DIR_TIMEOUT_SECONDS
        with self._lock:
            client = self._client
            session_id: str | None = None
            try:
                session_id = self._ensure_default_shell_session_id(client)

                kwargs = {
                    "command": remote_list_dir_command(resolved, max_depth),
                    "no_change_timeout": self._effective_no_change_timeout(timeout),
                    "hard_timeout": timeout,
                    "request_options": self._command_request_options(timeout),
                }
                if session_id is not None:
                    kwargs["id"] = session_id

                result = client.shell.exec_command(**kwargs)
            except httpx.TransportError as exc:
                # The response never arrived, so we cannot tell whether ``find``
                # is still running on the targeted shell generation. Fence that
                # generation and replay nothing. Local recovery ownership is
                # dropped before the cleanup attempt, so a failed cleanup can
                # never leave the ambiguous session reusable.
                self._default_shell_corrupted = True
                if session_id is not None:
                    self._recovery_session_id = None
                    self._cleanup_session_best_effort(
                        client,
                        session_id,
                        context="list_dir after transport failure",
                        request_options=self._bounded_cleanup_request_options(),
                    )
                logger.error(f"Failed to list directory in sandbox: {exc}")
                reason = "request timed out" if isinstance(exc, httpx.TimeoutException) else "transport failed"
                raise OSError(f"Failed to list directory '{resolved}': {reason}; directory result is unknown") from exc
            except ApiError as exc:
                if self._is_missing_shell_session_error(exc):
                    # The server has lost this generation. Forget it without replaying
                    # the listing; the next call creates a fresh recovery session.
                    self._default_shell_corrupted = True
                    self._recovery_session_id = None
                logger.error(f"Failed to list directory in sandbox: {exc}")
                raise OSError(f"Failed to list directory '{resolved}' in sandbox: {exc}") from exc
            except Exception as e:
                logger.error(f"Failed to list directory in sandbox: {e}")
                raise OSError(f"Failed to list directory '{resolved}' in sandbox: {e}") from e

            data = result.data if result else None
            if data is None:
                raise OSError(f"Failed to list directory '{resolved}' in sandbox: empty response")

            # Only a completed listing is a listing. ``list_dir`` returns
            # ``list[str]`` or raises; it never surfaces a partial ``find`` as
            # the directory's contents.
            status = getattr(data, "status", None)
            if status == "hard_timeout":
                raise TimeoutError(f"Failed to list directory '{resolved}': find timed out after {timeout:g} seconds; directory result may be incomplete")
            if self._is_session_invalidating_shell_status(status):
                # Same contract as an ambiguous transport timeout: fence the
                # generation that actually executed this listing, dropping local
                # recovery ownership before the bounded cleanup attempt.
                self._default_shell_corrupted = True
                if session_id is not None:
                    self._recovery_session_id = None
                    self._cleanup_session_best_effort(
                        client,
                        session_id,
                        context=f"list_dir after ambiguous status {status}",
                        request_options=self._bounded_cleanup_request_options(),
                    )
                raise OSError(f"Failed to list directory '{resolved}' in sandbox: command status '{status}'; directory result is unknown")

            return parse_remote_list_dir_output(
                data.output or "",
                resolved,
                pipeline_exit_code=getattr(data, "exit_code", None),
            )

    def write_file(self, path: str, content: str, append: bool = False) -> None:
        """Write content to a file in the sandbox.

        Args:
            path: The absolute path of the file to write to.
            content: The text content to write to the file.
            append: Whether to append the content to the file.
        """
        with self._lock:
            try:
                if append:
                    self._client.file.write_file(file=path, content=content, append=True)
                else:
                    self._client.file.write_file(file=path, content=content)
            except Exception as e:
                logger.error(f"Failed to write file in sandbox: {e}")
                raise

    def glob(self, path: str, pattern: str, *, include_dirs: bool = False, max_results: int = 200) -> tuple[list[str], bool]:
        if not include_dirs:
            result = self._client.file.find_files(path=path, glob=pattern)
            files = result.data.files if result.data and result.data.files else []
            filtered = [file_path for file_path in files if not should_ignore_path(file_path)]
            truncated = len(filtered) > max_results
            return filtered[:max_results], truncated

        result = self._client.file.list_path(path=path, recursive=True, show_hidden=False)
        entries = result.data.files if result.data and result.data.files else []
        matches: list[str] = []
        root_path = path.rstrip("/") or "/"
        root_prefix = root_path if root_path == "/" else f"{root_path}/"
        for entry in entries:
            if entry.path != root_path and not entry.path.startswith(root_prefix):
                continue
            if should_ignore_path(entry.path):
                continue
            rel_path = entry.path[len(root_path) :].lstrip("/")
            if path_matches(pattern, rel_path):
                matches.append(entry.path)
                # Look one match past the cap before deciding. Returning on
                # the max-th match cannot tell a listing that held exactly
                # ``max_results`` from one that held more, so an exhausted
                # listing was reported as truncated; it also returned a match
                # for ``max_results=0``. The ``include_dirs=False`` branch
                # below and the shared ``parse_remote_search_output`` path
                # decide the same way.
                if len(matches) > max_results:
                    return matches[:max_results], True
        return matches, False

    def grep(
        self,
        path: str,
        pattern: str,
        *,
        glob: str | None = None,
        literal: bool = False,
        case_sensitive: bool = False,
        max_results: int = 100,
    ) -> tuple[list[GrepMatch], bool]:
        import re as _re

        regex_source = _re.escape(pattern) if literal else pattern
        # Validate the pattern locally so an invalid regex raises re.error
        # (caught by grep_tool's except re.error handler) rather than a
        # generic remote API error.
        _re.compile(regex_source, 0 if case_sensitive else _re.IGNORECASE)
        total_cap = max(max_results * 4, max_results + 50)
        result = self._client.file.grep_files(
            path=path,
            pattern=pattern,
            case_insensitive=not case_sensitive,
            fixed_strings=literal,
            max_results=total_cap,
            max_file_size="1M",
            recursive=True,
        )
        data = result.data
        provider_matches = data.matches if data and data.matches else []
        root = path.rstrip("/") or "/"
        root_prefix = root if root == "/" else f"{root}/"

        matches: list[GrepMatch] = []
        truncated = bool(data and data.truncated)
        for match in provider_matches:
            file_path = match.file
            if should_ignore_path(file_path):
                continue
            if file_path == root:
                rel_path = file_path.rsplit("/", 1)[-1]
            elif file_path.startswith(root_prefix):
                rel_path = file_path[len(root_prefix) :]
            else:
                continue
            if glob is not None and not path_matches(glob, rel_path):
                continue
            matches.append(
                GrepMatch(
                    path=file_path,
                    line_number=match.line_number,
                    line=truncate_line(match.line_content),
                )
            )
            # Look one match past the cap before deciding, as ``glob`` above
            # does. Returning on the ``max_results``-th match cannot tell a
            # search that held exactly that many from one that held more, so an
            # exhausted search was reported as truncated.
            if len(matches) > max_results:
                return matches[:max_results], True

        return matches, truncated

    def update_file(self, path: str, content: bytes) -> None:
        """Update a file with binary content in the sandbox.

        Args:
            path: The absolute path of the file to update.
            content: The binary content to write to the file.
        """
        with self._lock:
            try:
                base64_content = base64.b64encode(content).decode("utf-8")
                self._client.file.write_file(file=path, content=base64_content, encoding="base64")
            except Exception as e:
                logger.error(f"Failed to update file in sandbox: {e}")
                raise
